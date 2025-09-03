import os
import ssl
import socket
from urllib.parse import urlparse

# 기본 경로 설정
BASE_DIR = "/home/woogaxon/WORK/DEV/RAG/DATA"
CERT_DIR = os.path.join(BASE_DIR, "certs")
os.makedirs(CERT_DIR, exist_ok=True)

def extract_domain(url):
    parsed = urlparse(url)
    return parsed.hostname

def save_certificate(domain):
    pem_path = os.path.join(CERT_DIR, f"{domain}.pem")
    try:
        conn = ssl.create_connection((domain, 443))
        context = ssl._create_unverified_context()  # 🔧 인증서 검증 끈 컨텍스트
        sock = context.wrap_socket(conn, server_hostname=domain)
        cert = ssl.DER_cert_to_PEM_cert(sock.getpeercert(True))
        with open(pem_path, "w") as f:
            f.write(cert)
        print(f"[CERT SAVED] {pem_path}")
        return pem_path
    except Exception as e:
        print(f"[CERT ERROR] {domain}: {e}")
        return None

