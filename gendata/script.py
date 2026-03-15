from pathlib import Path
import numpy as np
import polars as pl
import glob

def get_paths():
    current_dir = Path(__file__).parent
    project_root = current_dir.parent
    return {
        "user_file": project_root / "data" / "raw" / "users" / "big_data_userid.csv",
        "log_file": project_root / "data" / "raw" / "log_content" / "*.json",
        "output_dir": project_root / "data" / "raw" / "contracts",
        "output_file": project_root / "data" / "raw" / "contracts" / "dim_contract_mock.parquet",
    }

def generate_mock_contracts():
    paths = get_paths()

    # --- BƯỚC 1: Xử lý User ID (Trục chính) ---
    print("1. Đang xử lý danh sách User ID từ CSV...")
    # Đọc tất cả là String để giữ nguyên số 0 ở đầu (như 09123...)
    df_u = pl.read_csv(paths["user_file"], infer_schema_length=0)
    
    # Logic TRIM và NULLIF y hệt dbt
    df_u_clean = (
        df_u.select(pl.col("log_user_id").str.strip_chars().alias("user_id"))
        .filter(
            pl.col("user_id").is_not_null() & 
            (pl.col("user_id").str.to_uppercase() != "NULL") & 
            (pl.col("user_id") != "")
        )
        .unique()
    )
    
    # --- BƯỚC 2: Xử lý Contract từ Logs (Chỉ lấy cột cần thiết) ---
    print("2. Đang quét và trích xuất Contract từ logs...")
    log_files = glob.glob(str(paths["log_file"]))
    dfs = []
    
    for f in log_files:
        try:
            # Đọc JSON và unnest _source
            df = pl.read_ndjson(f).select(pl.col("_source").struct.unnest())
            
            # Chỉ lấy Contract và Mac nếu tồn tại, ép về String ngay
            valid_cols = []
            if "Contract" in df.columns: valid_cols.append("Contract")
            if "Mac" in df.columns: valid_cols.append("Mac")
            
            if valid_cols:
                df_sub = df.select(valid_cols)
                # Rename về chuẩn lowercase
                rename_dict = {c: c.lower() for c in valid_cols}
                df_sub = df_sub.rename(rename_dict)
                # Ép kiểu Utf8 để concat không lỗi Shape
                df_sub = df_sub.with_columns([pl.col(c).cast(pl.Utf8) for c in df_sub.columns])
                dfs.append(df_sub)
        except Exception as e:
            print(f"Bỏ qua file {f} do lỗi: {e}")

    if not dfs:
        print("Lỗi: Không tìm thấy dữ liệu contract nào!")
        return

    # Ghép diagonal để xử lý trường hợp file có Contract nhưng thiếu Mac hoặc ngược lại
    df_c_clean = (
        pl.concat(dfs, how="diagonal")
        .filter(
            pl.col("contract").is_not_null() & 
            (pl.col("contract") != "0") & 
            (pl.col("contract").str.to_uppercase() != "NULL")
        )
        .select([pl.col("contract").str.strip_chars(), pl.col("mac").str.strip_chars()])
        .unique()
    )

    print(f"   -> User sạch: {df_u_clean.height} | Contract sạch: {df_c_clean.height}")

    # --- BƯỚC 3 & 4: Quay số và Gán NULL ---
    print("3. Đang thực hiện ghép cặp ngẫu nhiên (Ưu tiên User ID)...")
    
    # Trộn User ngẫu nhiên
    df_u_shuffled = df_u_clean.sample(fraction=1.0, shuffle=True, seed=42)
    
    num_u = df_u_shuffled.height
    num_c = df_c_clean.height

    if num_c > num_u:
        print(f"Cảnh báo: Contract ({num_c}) nhiều hơn User ({num_u}). Lấy top bằng User.")
        df_c_clean = df_c_clean.head(num_u)
        num_c = num_u

    # Tách nhóm
    df_u_with_c = df_u_shuffled.head(num_c)
    df_u_leftover = df_u_shuffled.tail(num_u - num_c)

    # Ghép cặp (Phải cùng số dòng - Horizontal)
    df_part_1 = pl.concat([df_u_with_c, df_c_clean], how="horizontal")

    # Gán NULL cho phần thừa
    df_part_2 = df_u_leftover.with_columns([
        pl.lit(None, dtype=pl.Utf8).alias("contract"),
        pl.lit(None, dtype=pl.Utf8).alias("mac")
    ])

    # Hợp nhất Identity Map
    df_final = pl.concat([df_part_1, df_part_2], how="vertical")

    # Lưu kết quả
    paths["output_dir"].mkdir(parents=True, exist_ok=True)
    df_final.write_parquet(paths["output_file"])
    print(f"Xong! Customer 360 Spine hoàn tất: {df_final.height} dòng.")
    print(f"Lưu tại: {paths['output_file']}")

if __name__ == "__main__":
    generate_mock_contracts()
