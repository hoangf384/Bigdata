from pathlib import Path

import numpy as np
import polars as pl


def get_paths():
    current_dir = Path(__file__).parent
    project_root = current_dir.parent

    return {
        "user_file": project_root / "data" / "raw" / "users" / "big_data_userid.csv",
        "log_file": project_root / "data" / "raw" / "log_content" / "*.json",
        "output_dir": project_root / "data" / "raw" / "contracts",
        "output_file": project_root
        / "data"
        / "raw"
        / "contracts"
        / "dim_contract_mock.parquet",
    }


def load_users(user_file):
    df_user = pl.read_csv(user_file)

    total_users = df_user.height
    print(f"Tổng số User: {total_users}")

    df_user = df_user.with_columns(rn=pl.int_range(1, total_users + 1))

    return df_user, total_users


def load_logs(log_file):
    df_log = pl.read_ndjson(log_file).select(pl.col("_source").struct.unnest())
    return df_log


def extract_contracts(df_log):
    print("Đang lọc hợp đồng...")

    df_contract = (
        df_log.filter(pl.col("Contract").is_not_null())
        .select(["Contract", "Mac"])
        .unique(subset=["Contract", "Mac"])
    )

    return df_contract


def assign_random_user(df_contract, total_users):
    print("Đang quay số user...")

    random_array = np.random.randint(1, total_users + 1, size=df_contract.height)

    return df_contract.with_columns(random_rn=pl.Series("random_rn", random_array))


def join_user(df_contract, df_user):
    print("Đang ghép user với contract...")

    df_final = df_contract.join(
        df_user, left_on="random_rn", right_on="rn", how="inner"
    ).select(
        [
            "Contract",
            "Mac",
            pl.col("log_user_id").alias("user_id"),
        ]
    )

    return df_final


def save_output(df_final, output_dir, output_file):
    output_dir.mkdir(parents=True, exist_ok=True)

    df_final.write_parquet(output_file)

    print(f"Xong! Đã lưu {df_final.height} dòng tại: {output_file}")


def generate_mock_contracts():

    paths = get_paths()

    df_user, total_users = load_users(paths["user_file"])
    df_log = load_logs(paths["log_file"])

    df_contract = extract_contracts(df_log)
    df_contract = assign_random_user(df_contract, total_users)

    df_final = join_user(df_contract, df_user)

    save_output(df_final, paths["output_dir"], paths["output_file"])


if __name__ == "__main__":
    generate_mock_contracts()
