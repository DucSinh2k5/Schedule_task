from datetime import datetime, timezone

def main():
    now = datetime.now(timezone.utc)
    print(f"Task is running at {now.isoformat()}")

    # Viết code của bạn ở đây
    # Ví dụ: gọi API, gửi email, scrape nhẹ, tạo report...

if __name__ == "__main__":
    main()
