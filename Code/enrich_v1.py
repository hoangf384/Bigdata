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
        file extension hỗ trợ là "parquet"
    path : str
        đường dẫn trực tiếp.
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
    
    else:
        raise ValueError(f"Unsupported data_type: {data_type}")




def save_data(data: pl.DataFrame, path: str):
    """
    lưu parquet file
    """
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



def read_jsonl(path: str) -> pl.DataFrame:
    """
    Đọc file JSON Lines (JSONL) và chuyển đổi thành Polars DataFrame.

    Hàm này đọc file JSONL theo từng dòng, mỗi dòng tương ứng với một
    JSON object hợp lệ. Các dòng bị lỗi định dạng JSON sẽ bị bỏ qua
    (silent skip) và không làm gián đoạn quá trình đọc file.

    Parameters
    ----------
    path : str
        Đường dẫn tới file JSONL cần đọc.

    Returns
    -------
    polars.DataFrame
        DataFrame chứa toàn bộ các JSON object hợp lệ trong file.
        Schema được suy luận động dựa trên dữ liệu đọc được.
    Note
    Hàm không raise lỗi, có thể sẽ sửa sau
    """

    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return pl.DataFrame(rows)



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

# --------------------------
# LLM handling
# --------------------------


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
# prompt helpers
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

# ---------------------------
# Multithread Asyncio
# --------------------------

async def classify_batch_async(movie_list: list[str]) -> dict[str, str]:
    """
    Phân loại danh sách movie/keyword theo batch bằng LLM (async).

    Hàm này nhận vào một danh sách keyword (movie_list), xây dựng prompt,
    gọi LLM để phân loại từng keyword sang category tương ứng.
    Kết quả trả về là một dictionary ánh xạ keyword -> category.

    Trong trường hợp:
    - input rỗng
    - LLM trả về JSON không hợp lệ
    - lỗi network / timeout / parse
    hàm sẽ fallback toàn bộ keyword về category "Other"

    Parameters
    ----------
    movie_list : list[str]
        Danh sách keyword hoặc tên movie cần phân loại.
        Mỗi phần tử được coi là một đơn vị phân loại độc lập.

    Returns
    -------
    dict[str, str]
        Dictionary ánh xạ:
            { keyword : category }

    Notes
    -----
    - Hàm sử dụng LLM theo cơ chế async để tối ưu throughput
      khi xử lý dữ liệu lớn theo batch.
    - Kết quả LLM được yêu cầu ở định dạng JSON (`response_format=json_object`)
      và được làm sạch lại bằng `clean_llm_json` trước khi parse.
    """

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

        return {k: parsed.get(k, "Other") for k in movie_list}

    except Exception as e:
        print("LLM JSON parse error:", e)
        return {k: "Other" for k in movie_list}


async def api_worker(i: int, batch: list[str], semaphore: asyncio.Semaphore, queue: asyncio.Queue) -> None:
    """
    async worker gọi LLM phân loại keyword theo batch
    và đẩy kết quả vào queue.

    Hàm này hoạt động như một worker trong mô hình producer–consumer:
    - Nhận một batch keyword
    - Giới hạn số lượng request đồng thời bằng semaphore
    - Gọi LLM async để phân loại keyword
    - Đưa kết quả từng keyword vào queue dưới dạng JSONL

    Hàm được thiết kế để chạy song song nhiều instance,
    phục vụ pipeline xử lý dữ liệu lớn với kiểm soát concurrency.

    Parameters
    ----------
    i : int
        Chỉ số batch (index), dùng cho mục đích logging / theo dõi tiến độ.
    batch : list[str]
        Danh sách keyword cần phân loại trong một batch
        (ví dụ: 500 keyword / batch).
    semaphore : asyncio.Semaphore
        Semaphore dùng để giới hạn số worker gọi API LLM
        chạy đồng thời, tránh vượt rate limit hoặc quá tải hệ thống.
    queue : asyncio.Queue
        Queue bất đồng bộ dùng để truyền kết quả downstream.
        Mỗi phần tử trong queue là một dòng JSON (JSONL)
        biểu diễn mapping keyword -> category.

    Returns
    -------
    None
        Hàm không trả về giá trị; kết quả được đẩy vào `queue`
        để consumer xử lý tiếp (ví dụ: ghi file, ghi DB).

    Notes
    -----
    - Hàm sử dụng `async with semaphore` để đảm bảo số request
      LLM đồng thời không vượt quá ngưỡng cho phép.
    - Kết quả phân loại được serialize sang JSON string
      với `ensure_ascii=False` để hỗ trợ tiếng Việt.
    - Logging tiến độ được thực hiện mỗi 5 batch
      (tương đương mỗi 2500 keyword nếu batch size = 500).
    - Hàm không xử lý exception nội bộ; giả định rằng
      `classify_batch_async` đã có cơ chế fallback an toàn.
    """
   
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
    """
    Consumer bất đồng bộ để ghi dữ liệu từ queue ra file theo cơ chế buffered I/O.

    Hàm này đóng vai trò consumer trong mô hình producer–consumer:
    - Nhận từng dòng dữ liệu (JSONL) từ asyncio.Queue
    - Gom dữ liệu theo buffer để giảm số lần ghi đĩa
    - Ghi dữ liệu ra file theo chế độ append
    - Đảm bảo flush và sync để tránh mất dữ liệu khi pipeline bị gián đoạn

    Hàm chạy vô hạn cho đến khi nhận được sentinel `None`,
    khi đó sẽ ghi nốt dữ liệu còn tồn trong buffer và kết thúc.

    Parameters
    ----------
    queue : asyncio.Queue
        Queue bất đồng bộ chứa dữ liệu đầu vào.
        Mỗi phần tử trong queue là một chuỗi (string),
        thường là một dòng JSONL đã được serialize sẵn.
    path : str
        Đường dẫn tới file output.
        File sẽ được mở ở chế độ append (`"a"`).
    buffer_size : int, optional
        Số dòng tối đa trong buffer trước khi ghi ra đĩa.
        Giá trị mặc định là 100.

    Returns
    -------
    None
        Hàm không trả về giá trị; 
    Notes
    -----
    - Sử dụng buffer để giảm I/O syscall khi ghi file,
      giúp tăng hiệu năng khi xử lý dữ liệu lớn.
    - Gọi `flush()` và `os.fsync()` sau mỗi lần ghi
      để đảm bảo dữ liệu đã được ghi xuống disk.
    - Sentinel `None` được dùng để báo hiệu kết thúc producer,
      đảm bảo consumer thoát vòng lặp một cách an toàn.
    - Hàm được thiết kế để chạy song song với nhiều producer
      nhưng chỉ nên có một writer cho mỗi file output.
    """

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


# --------------------------
#
# --------------------------

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
    """
    Điều phối toàn bộ pipeline phân loại keyword bằng LLM theo cơ chế bất đồng bộ.

    Hàm này đóng vai trò orchestrator cho pipeline ETL + LLM enrichment,
    thực hiện các bước chính:
    1. Đọc dữ liệu đầu vào (parquet)
    2. Trích xuất danh sách keyword cần phân loại
    3. Loại bỏ các keyword đã được phân loại trước đó (checkpoint / resume)
    4. Chia keyword thành các batch
    5. Khởi chạy các worker async để gọi LLM
    6. Ghi kết quả ra file JSONL theo cơ chế queue-based
    7. Hợp nhất kết quả phân loại vào dữ liệu gốc và lưu output cuối cùng

    Pipeline được thiết kế để:
    - Chạy song song với giới hạn concurrency
    - Hỗ trợ resume khi bị gián đoạn
    - Không làm crash toàn bộ flow khi LLM gặp lỗi
    - Ghi dữ liệu theo dạng streaming để tiết kiệm bộ nhớ

    Returns
    -------
    None
        Hàm không trả về giá trị; kết quả được ghi ra file
        parquet ở bước cuối cùng.
    """
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
    semaphore = asyncio.Semaphore(8)

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
        print("Interrupted safely")

    await queue.join()      
    await queue.put(None)   
    await writer



if __name__ == "__main__": 
    start = time.time()
    
    asyncio.run(control_flow_async())
    
    end = time.time()
    print(f"Total time: {(end - start)/60:.2f} minutes")