import os, time, subprocess, logging, json, requests
from pythonjsonlogger import jsonlogger

logger = logging.getLogger("lag-watcher")
h = logging.StreamHandler()
h.setFormatter(jsonlogger.JsonFormatter())
logger.addHandler(h)
logger.setLevel(os.getenv("LOG_LEVEL","INFO"))

BROKER = os.getenv("KAFKA_BROKER","kafka:9092")
GROUPS = os.getenv("GROUPS","").split(",")
THRESH = int(os.getenv("LAG_THRESHOLD","1000"))
POLL = int(os.getenv("POLL_SECONDS","30"))
WEBHOOK = os.getenv("ALERT_WEBHOOK_URL","").strip()

def alert(text: str):
    if not WEBHOOK:
        return
    try:
        requests.post(WEBHOOK, json={"text": text}, timeout=3)
    except Exception:
        logger.warning("webhook post failed")

def describe(group: str) -> int:
    cmd = [
        "bash","-lc",
        f"kafka-consumer-groups --bootstrap-server {BROKER} --describe --group {group} | tail -n +2"
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=10).decode()
        total_lag = 0
        for line in out.strip().splitlines():
            parts = [p for p in line.split() if p]
            if len(parts) >= 6:
                lag = parts[-1]
                try:
                    total_lag += int(lag)
                except ValueError:
                    pass
        return total_lag
    except subprocess.CalledProcessError as e:
        logger.warning("describe failed for %s: %s", group, e.output.decode())
        return -1
    except Exception as e:
        logger.warning("describe exception: %s", e)
        return -1

def main():
    logger.info("Lag watcher started. groups=%s threshold=%d", GROUPS, THRESH)
    while True:
        for g in GROUPS:
            g = g.strip()
            if not g: continue
            lag = describe(g)
            if lag >= 0:
                logger.info(json.dumps({"group": g, "lag": lag}))
                if lag > THRESH:
                    alert(f":warning: Consumer lag high for `{g}` = {lag} (> {THRESH})")
        time.sleep(POLL)

if __name__ == "__main__":
    main()
