import os
import base64
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


def load_env_variables(env_vars: list):
    """
    Load environment variables from a list of strings.
    :param env_vars: List of strings in the format "KEY=VALUE"
    """
    for var in env_vars:
        if not var or var.strip().startswith("#"):
            continue
        if "=" not in var:
            continue
        key, value = var.split("=", 1)
        os.environ[key.strip()] = value.strip()


def env_cipher(env_bytes: bytes, action: str = "encrypt"):
    """
    Parse .env data in bytes and set environment variables after encrypting or decrypting them.
    :param env_bytes: bytes containing environment variables in .env format
    :param action: "encrypt" or "decrypt"
    :return: bytes containing the processed environment variables
    """
    action_method = encrypt if action == "encrypt" else decrypt
    lines = env_bytes.decode("utf-8").splitlines()
    keys_to_skip = ["ENCRYPT_KEY", "ENCRYPT_IV_SECRET"]

    # Load environment variables to encrypt/decrypt
    for key in keys_to_skip:
        line = next((line for line in lines if line.startswith(key + "=")), None)
        if not line or line.strip().startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ[key.strip()] = value.strip()

    result = []
    for line in lines:
        if not line or line.strip().startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        output_value = value
        if key.strip() not in keys_to_skip:
            output_value = action_method(value)
        result.append(f"{key.strip()}={output_value}")
    return "\n".join(result).encode("utf-8")


def encrypt_env_file(input_path):
    if not os.path.exists(input_path):
        return

    output_dirs = os.path.dirname(input_path)
    output_path = os.path.join(output_dirs, ".env.enc")
    encrypted = None
    with open(input_path, "rb") as f:
        data = f.read()
        encrypted = env_cipher(data, action="encrypt")

    if not encrypted:
        return

    os.makedirs(output_dirs, exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(encrypted)
    return output_path


def decrypt_env_file(input_path, file_output: bool = True):
    if not os.path.exists(input_path):
        return

    output_dirs = input_path.split("/")[:-2]
    output_path = os.path.join(*output_dirs, ".env.dec")
    data = None
    with open(input_path, "rb") as f:
        file_content = f.read()
        data = env_cipher(file_content, action="decrypt")

    if not data:
        return

    if not file_output:
        data = data.decode("utf-8").splitlines()
        return data

    parent_dir = os.path.dirname(input_path)
    if parent_dir and not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
    with open(output_path, "wb") as f:
        f.write(data)
    return output_path


def encrypt(value):
    try:
        encrypt_key = os.environ.get("ENCRYPT_KEY", "")
        encrypt_iv_key = os.environ.get("ENCRYPT_IV_SECRET", "")
        value = pad(value.encode(), 16)
        cipher = AES.new(
            encrypt_key.encode("utf-8"), AES.MODE_CBC, encrypt_iv_key.encode("utf-8")
        )
        encrypt_data = base64.b64encode(cipher.encrypt(value))
        return encrypt_data.decode("utf-8", "ignore")
    except:
        return value


def decrypt(encrypt_value, use_license_key=False):
    try:
        if not encrypt_value:
            return ""
        encrypt_key = os.environ.get("ENCRYPT_KEY", "")
        encrypt_iv_key = os.environ.get("ENCRYPT_IV_SECRET", "")
        if use_license_key:
            encrypt_key = os.environ.get("LICENSE_ENCRYPT_KEY", "")
            encrypt_iv_key = os.environ.get("LICENSE_ENCRYPT_IV_SECRET", "")
        enc = base64.b64decode(encrypt_value)
        cipher = AES.new(
            encrypt_key.encode("utf-8"), AES.MODE_CBC, encrypt_iv_key.encode("utf-8")
        )
        decrypt_data = unpad(cipher.decrypt(enc), 16)
        return decrypt_data.decode("utf-8", "ignore")
    except:
        return encrypt_value


def main(action: str, input_file: str, type: str):
    action = action if action else "encrypt"
    input_file = input_file if input_file else ""
    if not input_file or not action:
        return

    action_method = encrypt_env_file if action == "encrypt" else decrypt_env_file
    if type and type.lower() == "value":
        current_dir = os.path.dirname(os.path.abspath(__file__))
        env_file_path = os.path.join(current_dir, ".env.enc")
        if not os.path.exists(env_file_path):
            env_file_path = os.path.join(current_dir, ".env")
        env_vars = decrypt_env_file(env_file_path, file_output=False)
        load_env_variables(env_vars)
        action_method = encrypt if action == "encrypt" else decrypt

    result = action_method(input_file)
    if type and type.lower() == "value":
        print(result)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Encrypt or decrypt .env file")
    parser.add_argument("--action", choices=["encrypt", "decrypt"])
    parser.add_argument("--input", default="")
    parser.add_argument("--type", default="", required=False)
    args = parser.parse_args()
    main(args.action, args.input, args.type)
