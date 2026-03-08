import base64
import os
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


def encrypt(value):
    try:
        if not value:
            return value
        value = pad(value.encode(), 16)
        cipher = AES.new(
            os.environ.get("ENCRYPT_KEY").encode("utf-8"),
            AES.MODE_CBC,
            os.environ.get("ENCRYPT_IV_SECRET").encode("utf-8"),
        )
        encrypt_data = base64.b64encode(cipher.encrypt(value))
        return encrypt_data.decode("utf-8", "ignore")
    except:
        return value


def decrypt(encrypt_value):
    try:
        if not encrypt_value:
            return encrypt_value
        enc = base64.b64decode(encrypt_value)
        cipher = AES.new(
            os.environ.get("ENCRYPT_KEY").encode("utf-8"),
            AES.MODE_CBC,
            os.environ.get("ENCRYPT_IV_SECRET").encode("utf-8"),
        )
        decrypt_data = unpad(cipher.decrypt(enc), 16)
        return decrypt_data.decode("utf-8", "ignore")
    except:
        return encrypt_value
