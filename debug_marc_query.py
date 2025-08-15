
from pymarc import MARCReader


def main():
    try:
        print("---" + "cimb.marc (Holdings)" + "---")
        with open('cimb.marc', 'rb') as fh:
            reader = MARCReader(fh)
            record = next(reader)
            if record:
                print(record)

        print("\n---" + "cimb_bibliographic.marc (Bibliographic)" + "---")
        with open('cimb_bibliographic.marc', 'rb') as fh:
            reader = MARCReader(fh)
            record = next(reader)
            if record:
                print(record)

    except FileNotFoundError as e:
        print(f"Error: {e.filename} not found. Please make sure the file is in the same directory as the script.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
