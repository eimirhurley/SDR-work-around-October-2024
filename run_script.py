import sys
import subprocess


def main():
    if len(sys.argv) != 2:
        print("Usage: python run_script.py <token>")
        sys.exit(1)

    token = sys.argv[1]
    subprocess.run(["python", "main_script.py", token])


if __name__ == "__main__":
    main()
