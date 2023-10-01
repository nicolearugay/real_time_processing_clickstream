import time

def monitor_log_file(log_file_path):
    with open(log_file_path, 'r') as f:
        f.seek(0, 2)
        while True:
            # Read new line
            line = f.readline()
            # If line is not empty
            if line:
                if "ERROR" in line:
                    print(f"Alert: An error was logged: {line}")
                    # You can send email here
            time.sleep(1)

if __name__ == '__main__':
    monitor_log_file('application.log')


