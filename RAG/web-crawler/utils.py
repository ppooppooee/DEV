from urllib.parse import unquote
import re

def udprint(*args, **kwargs):
    def decode_url_in_text(text):
        if not isinstance(text, str):
            return text
        # URL 패턴 찾기 (http로 시작하는 퍼센트 인코딩된 문자열)
        return re.sub(r'https?://[^\s]+', lambda m: unquote(m.group()), text)

    decoded_args = [decode_url_in_text(a) for a in args]
    print(*decoded_args, **kwargs)
