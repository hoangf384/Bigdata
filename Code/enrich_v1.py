import json
import glob
import os
import time
import asyncio
from dotenv import load_dotenv
from openai import OpenAI, AsyncOpenAI
import polars as pl

# API workers (N)  ──►  Queue  ──►  Writer (1)  ──►  mapping.jsonl


# ---------------------------
# init
# ---------------------------

load_dotenv()
client = OpenAI(api_key = os.getenv("OPENAI_API_KEY"))
async_client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ---------------------------
# path
# ---------------------------

folder_path = "./Data/raw/log_search/"
save_path = "./Data/destination/log_search/category/"


# ---------------------------
# IO helpers
# ---------------------------


def read_data(data_type: str, path: str) -> pl.DataFrame:
    """
    đọc dữ liệu và trả về dataframe

    Parameters
    ----------
    data_type : str
        file extension hỗ trợ là "parquet", "jsonl".
    path : str
        đường dẫn trực tiếp (đối với parquet) hoặc đường dẫn dán tiếp (tên file đối với jsonl) 

    Returns
    -------
    polars.DataFrame

    Raises
    ------
    ValueError
        If input files are missing or data_type is unsupported.
    """

    if data_type == "parquet":
        files = glob.glob(os.path.join(path, "**/*.parquet"), recursive=True)

        if not files:
            raise ValueError(f"No parquet files found under path: {path}")

        try:
            data = pl.read_parquet(files)
            return data
        except Exception as e:
            raise RuntimeError(f"Failed to read parquet files: {e}") from e


    elif data_type == "jsonl":
        

        if not os.path.exists(path):
            raise ValueError(f"JSONL file not found: {path}")

        if os.path.getsize(path) == 0:
            
            return pl.DataFrame(
                {
                    "keyword": pl.Series([], dtype=pl.Utf8),
                    "category": pl.Series([], dtype=pl.Utf8),
                }
            )
        return pl.read_ndjson(path)
    
    else:
        raise ValueError(f"Unsupported data_type: {data_type}")




def save_data(data, path):
    """
    Docstring for save_data
    
    :param data: Description
    :param path: Description
    """
    # hỏi chatGPT để biết được () cần tham số gì
    data.write_parquet(path)



def clean_llm_json(text: str) -> str:
    """
    Làm sạch output JSON từ LLM, loại bỏ markdown nếu còn sót.

    Parameters
    ----------
    text : str
        Chuỗi output từ LLM.

    Returns
    -------
    str
        Chuỗi JSON sạch, sẵn sàng để parse.
    """
    text = text.strip()

    if text.startswith("```"):
        text = text.replace("```json", "").replace("```", "").strip()

    return text



def read_jsonl(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return pl.DataFrame(rows)


# ---------------------------
# transform
# ---------------------------


def get_data(data: pl.DataFrame) -> list[str]:
    """
    Trích xuất danh sách keyword duy nhất từ DataFrame đầu vào.

    Parameters
    ----------
    data : polars.DataFrame
        DataFrame chứa dữ liệu log, bắt buộc có cột `keyword`.

    Returns
    -------
    list of str
        Danh sách keyword (kiểu Python list), đã:
        - loại bỏ giá trị null
        - loại bỏ trùng lặp
    """
    keywords = (
        data.select("keyword")
            .drop_nulls()
            .unique()
            .to_series()
            .to_list()
    )
    return keywords

#
#
#


def init_output_folder(path: str) -> None:
    """
    Khởi tạo thư mục output và file mapping nếu chưa tồn tại.
    """
    if os.path.exists(path) and not os.path.isdir(path):
        raise ValueError(f"Output path exists but is not a directory: {path}")

    os.makedirs(path, exist_ok=True)

    mapping_file = os.path.join(path, "mapping.jsonl")
    if not os.path.exists(mapping_file):
        with open(mapping_file, "w", encoding="utf-8"):
            pass



def chunks(lst: list, size: int):
    """
    Chia một danh sách thành các batch con có kích thước cố định.

    Parameters
    ----------
    lst : list
        Danh sách đầu vào cần được chia nhỏ.
    size : int
        Kích thước tối đa của mỗi batch.

    Yields
    ------
    list
        Các danh sách con (batch) có độ dài không vượt quá `size`.

    Notes
    -----
    - Batch cuối cùng có thể có số phần tử nhỏ hơn `size`.
    - Hàm này thường được dùng để:
        * chia dữ liệu xử lý theo lô (batch processing)
        * giới hạn số phần tử khi gọi API hoặc LLM
        * tránh vượt quá memory hoặc rate limit
    """
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


# ----------------------------
# for function
# ----------------------------

def build_prompt(movie_list):
    return f"""Bạn là một chuyên gia phân loại nội dung phim, chương trình truyền hình và nội dung giải trí tại Việt Nam.

Bạn sẽ nhận một danh sách keyword tìm kiếm, có thể:
- viết sai chính tả
- không dấu
- viết liền
- viết tắt
- hoặc chỉ là cụm từ gợi ý mơ hồ

⚠️ NGUYÊN TẮC BẮT BUỘC (CỰC KỲ QUAN TRỌNG):
- MỖI keyword PHẢI được gán CHÍNH XÁC 1 thể loại.
- TUYỆT ĐỐI KHÔNG trả về "Other" nếu còn bất kỳ cách suy đoán hợp lý nào.
- "Other" CHỈ được dùng khi keyword hoàn toàn vô nghĩa, spam, hoặc không liên quan nội dung giải trí.

NHIỆM VỤ:
1. Chuẩn hoá và sửa lỗi keyword (CHỈ để suy luận nội bộ, KHÔNG ghi ra output).
2. Nhận diện ý nghĩa gốc gần đúng nhất (phim, show, bài hát, sự kiện, đội tuyển, mô tả nội dung).
3. Gán thể loại PHÙ HỢP NHẤT trong danh sách dưới đây.

DANH SÁCH THỂ LOẠI HỢP LỆ (CHỈ ĐƯỢC CHỌN 1):
- Action
- Romance
- Comedy
- Horror
- Animation
- Drama
- C Drama
- K Drama
- Sports
- Music
- Reality Show
- TV Channel
- News
- Other

LUẬT SUY DIỄN ƯU TIÊN (PHẢI TUÂN THEO):
- Có "tập", "episode", "ep":
    • Nếu keyword chứa tên show / gameshow / reality quen thuộc
      (ví dụ: running man, 2 ngày 1 đêm, rap việt, the voice, masterchef)
      → Reality Show
    • Nếu chứa từ khoá phim / series / hành động
      → Drama hoặc Action
- Karaoke, bài hát, ca sĩ, lời bài hát, remix → Music
- Trận đấu, bóng đá, đội tuyển, U19, Việt Nam vs → Sports
- Tên phim, series, tiêu đề truyện (kể cả mơ hồ, viết sai) → Drama
- Phim Trung Quốc → C Drama
- Phim Hàn Quốc → K Drama
- Show truyền hình, gameshow → Reality Show
- Tên kênh (VTV, HTV, K+, HBO, Channel) → TV Channel
- Keyword ngắn giống tên riêng / tiêu đề → ưu tiên Drama hoặc Music
- Chỉ dùng Other khi keyword không thể gán vào bất kỳ nhóm nào ở trên

OUTPUT:
- Chỉ trả về 1 JSON object
- Key = keyword gốc trong danh sách (KHÔNG sửa)
- Value = thể loại đã phân loại
- KHÔNG giải thích, KHÔNG thêm text ngoài JSON

Danh sách keyword:
{movie_list}
"""

async def classify_batch_async(movie_list):
    if not movie_list:
        return {}

    prompt = build_prompt(movie_list)

    try:
        resp = await async_client.chat.completions.create(
            model="gpt-5-nano",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )

        text = resp.choices[0].message.content
        text = clean_llm_json(text)

        parsed = json.loads(text)

        # 🔒 đảm bảo không thiếu key
        return {k: parsed.get(k, "Other") for k in movie_list}

    except Exception as e:
        print("LLM JSON parse error:", e)
        return {k: "Other" for k in movie_list}


# def classify_batch(movie_list):
#     """
#     Docstring for classify_batch
    
#     :param movie_list: Description
#     """
#     if not movie_list:
#         return {}
    
#     prompt = f"""Bạn là một chuyên gia phân loại nội dung phim, chương trình truyền hình và nội dung giải trí tại Việt Nam.

# Bạn sẽ nhận một danh sách keyword tìm kiếm, có thể:
# - viết sai chính tả
# - không dấu
# - viết liền
# - viết tắt
# - hoặc chỉ là cụm từ gợi ý mơ hồ

# ⚠️ NGUYÊN TẮC BẮT BUỘC (CỰC KỲ QUAN TRỌNG):
# - MỖI keyword PHẢI được gán CHÍNH XÁC 1 thể loại.
# - TUYỆT ĐỐI KHÔNG trả về "Other" nếu còn bất kỳ cách suy đoán hợp lý nào.
# - "Other" CHỈ được dùng khi keyword hoàn toàn vô nghĩa, spam, hoặc không liên quan nội dung giải trí.

# NHIỆM VỤ:
# 1. Chuẩn hoá và sửa lỗi keyword (CHỈ để suy luận nội bộ, KHÔNG ghi ra output).
# 2. Nhận diện ý nghĩa gốc gần đúng nhất (phim, show, bài hát, sự kiện, đội tuyển, mô tả nội dung).
# 3. Gán thể loại PHÙ HỢP NHẤT trong danh sách dưới đây.

# DANH SÁCH THỂ LOẠI HỢP LỆ (CHỈ ĐƯỢC CHỌN 1):
# - Action
# - Romance
# - Comedy
# - Horror
# - Animation
# - Drama
# - C Drama
# - K Drama
# - Sports
# - Music
# - Reality Show
# - TV Channel
# - News
# - Other

# LUẬT SUY DIỄN ƯU TIÊN (PHẢI TUÂN THEO):
# - Có "tập", "episode", "ep":
#     • Nếu keyword chứa tên show / gameshow / reality quen thuộc
#       (ví dụ: running man, 2 ngày 1 đêm, rap việt, the voice, masterchef)
#       → Reality Show
#     • Nếu chứa từ khoá phim / series / hành động
#       → Drama hoặc Action
# - Karaoke, bài hát, ca sĩ, lời bài hát, remix → Music
# - Trận đấu, bóng đá, đội tuyển, U19, Việt Nam vs → Sports
# - Tên phim, series, tiêu đề truyện (kể cả mơ hồ, viết sai) → Drama
# - Phim Trung Quốc → C Drama
# - Phim Hàn Quốc → K Drama
# - Show truyền hình, gameshow → Reality Show
# - Tên kênh (VTV, HTV, K+, HBO, Channel) → TV Channel
# - Keyword ngắn giống tên riêng / tiêu đề → ưu tiên Drama hoặc Music
# - Chỉ dùng Other khi keyword không thể gán vào bất kỳ nhóm nào ở trên

# OUTPUT:
# - Chỉ trả về 1 JSON object
# - Key = keyword gốc trong danh sách (KHÔNG sửa)
# - Value = thể loại đã phân loại
# - KHÔNG giải thích, KHÔNG thêm text ngoài JSON

# Danh sách keyword:
# {movie_list}
#     """

#     try:
#         resp = client.chat.completions.create(
#             model="gpt-5-nano",
#             messages=[{"role": "user", "content": prompt}],
#             response_format={"type": "json_object"}  
#         )

#         text = resp.choices[0].message.content
#         text = clean_llm_json(text)

#         parsed = json.loads(text)

#         # Đảm bảo đủ key cho toàn batch
#         return {k: parsed.get(k, "Other") for k in movie_list}

#     except Exception as e:
#         print("LLM JSON parse error:", e)
#         return {k: "Other" for k in movie_list}


async def api_worker(i, batch, semaphore, queue):
    async with semaphore:
        mapping = await classify_batch_async(batch)

        for k, v in mapping.items():
            await queue.put(
                json.dumps(
                    {"keyword": k, "category": v},
                    ensure_ascii=False
                ) + "\n"
            )

        if (i+1) % 5 == 0:
                print(f"Processed {(i + 1) * 500} keywords")


async def writer_worker(queue, path, buffer_size=100):
    buffer = []
    with open(path, "a", encoding="utf-8") as f:
        while True:
            line = await queue.get()
            if line is None:
                if buffer:
                    f.write("".join(buffer))
                    f.flush()
                    os.fsync(f.fileno())
                queue.task_done()
                break

            buffer.append(line)
            if len(buffer) >= buffer_size:
                f.write("".join(buffer))
                f.flush()
                os.fsync(f.fileno())
                buffer = []

            queue.task_done()




# def save_mapping(data, path):
#     """
#     Docstring for save_mapping
    
#     :param data: Description
#     :param path: Description
#     """
#     with open(path, "a", encoding="utf-8") as f:
#         for k, v in data.items():
#             f.write(
#                 json.dumps(
#                     {"keyword": k, "category": v},
#                     ensure_ascii=False
#                 ) + "\n"
#             )
#         f.flush()
#         os.fsync(f.fileno())

# ----------------------
# join 
# ----------------------


def join_category(data: pl.DataFrame, mapping_df: pl.DataFrame) -> pl.DataFrame:
    """
    Ghép thông tin category vào dữ liệu gốc theo cột keyword.

    Parameters
    ----------
    data : polars.DataFrame
        DataFrame dữ liệu gốc, bắt buộc có cột `keyword`.
    mapping_df : polars.DataFrame
        DataFrame mapping keyword–category, gồm các cột:
        - keyword
        - category

    Returns
    -------
    polars.DataFrame
        DataFrame đầu ra sau khi được bổ sung cột `category`
    """
    data = data.join(
        mapping_df,
        on="keyword",
        how="left"
    )
    return data



# --------------------------
# control_flow
# --------------------------

async def control_flow_async():
    data = read_data("parquet", folder_path)
    keywords = get_data(data)
    init_output_folder(save_path)

    mapping_path = save_path + "mapping.jsonl"

    if os.path.exists(mapping_path):
        mapping_df = read_jsonl(mapping_path)
        classified = set(mapping_df["keyword"].to_list())
        keywords = [k for k in keywords if k not in classified]

    batches = list(chunks(keywords, 500))

    queue = asyncio.Queue(maxsize=2000)
    semaphore = asyncio.Semaphore(8)   # 🔥 N API WORKERS

    writer = asyncio.create_task(
        writer_worker(queue, mapping_path)
    )

    api_tasks = [
        asyncio.create_task(api_worker(i, batch, semaphore, queue))
        for i, batch in enumerate(batches)
    ]

    try:
        await asyncio.gather(*api_tasks, return_exceptions=True)
        
    except KeyboardInterrupt:
        print("⏹ Interrupted safely")

    await queue.join()      # 🔒 chờ ghi xong toàn bộ
    await queue.put(None)   # kết thúc writer
    await writer

    # FINAL STEP (GIỮ NGUYÊN)
    mapping_df = read_jsonl(mapping_path).unique(
        subset=["keyword"], keep="last"
    )

    final_df = join_category(data, mapping_df)
    save_data(final_df, save_path + "final.parquet")




# def control_flow():
#     """
#     Docstring for control_flow
#     """
#     # 1. read, transform and init folder
#     data = read_data("parquet", folder_path)
#     keywords = get_data(data)
#     init_output_folder(save_path)

#     mapping_path = save_path + "mapping.jsonl"

#     if os.path.exists(mapping_path):
#         mapping_df = read_data("jsonl", mapping_path)
#         classified = set(mapping_df["keyword"].to_list())
#         keywords = [k for k in keywords if k not in classified]


#     for batch in chunks(keywords, 500):
        
#         # 2.1 Call LLM for this batch
#         mapping = classify_batch(batch)

#         # 2.2 Save mapping immediately (checkpoint)
#         save_mapping(mapping, save_path + "mapping.jsonl")

#         # Logging
#         time.sleep(0.2)
#         print(f"Processed {len(mapping)} keywords")

#     # 3. Load mapping, deduplicate, join, write output
#     mapping_df = read_jsonl(save_path + "mapping.jsonl")

#     mapping_df = (
#         mapping_df
#         .unique(subset=["keyword"], keep="last")
#     )

#     final_df = join_category(data, mapping_df)
#     save_data(final_df, save_path + "final.parquet")


# def control_flow_test():
#     """
#     Test run: chỉ chạy 1 batch đầu tiên (10 keywords)
#     """

#     # 1. Read data
#     data = read_data("parquet", folder_path)
#     keywords = get_data(data)

#     print(f"Total unique keywords: {len(keywords)}")

#     # 🔹 chỉ lấy 10 keyword đầu
#     test_keywords = keywords[:10]
#     print("Test keywords:", test_keywords)

#     # 2. Call LLM
#     mapping = classify_batch(test_keywords)

#     print("LLM output mapping:")
#     print(mapping)

#     # 3. Save thử ra file test
#     test_path = save_path + "mapping_test.jsonl"
#     os.makedirs(save_path, exist_ok=True)

#     save_mapping(mapping, test_path)

#     print(f"✅ Saved test mapping to {test_path}")



if __name__ == "__main__": 
    start = time.time()
    
    asyncio.run(control_flow_async())
    
    end = time.time()
    print(f"Total time: {(end - start)/60:.2f} minutes")