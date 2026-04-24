import gspread
from google.oauth2.service_account import Credentials

SHEET_NAME = "FB Groups Input"
WORKSHEET_NAME = "Trang tính1"
CREDS_FILE = "creds.json"


def get_group_urls():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)

    sheet = client.open(SHEET_NAME)
    worksheet = sheet.worksheet(WORKSHEET_NAME)

    records = worksheet.get_all_records()

    group_urls = []
    for row in records:
        url = str(row.get("group_url", "")).strip()
        if url:
            group_urls.append(url)

    return group_urls


if __name__ == "__main__":
    groups = get_group_urls()
    print("Danh sách group đọc được:")
    for i, group in enumerate(groups, 1):
        print(f"{i}. {group}")
