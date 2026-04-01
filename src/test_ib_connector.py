import paramiko
import gnupg
import os
import zipfile

# --- IBKR CREDENTIALS ---
HOST = 'ftp2.interactivebrokers.com'
USERNAME = 'gaardcapital'
REMOTE_DIR = 'outgoing'

# --- YOUR LOCAL PATHS ---
SSH_KEY_PATH = '/Users/michaelmolenaar/Desktop/BROGAARD/Gaard/Gaard Reporting System/data/Gaard_Keys/IB_SSH_Private.txt'
PGP_KEY_PATH = '/Users/michaelmolenaar/Desktop/BROGAARD/Gaard/Gaard Reporting System/data/Gaard_Keys/IB_PGP_Private.txt'
DOWNLOAD_DIR = '/Users/michaelmolenaar/Desktop/BROGAARD/Gaard/Gaard Reporting System/data/test_downloads'

def run_test():
    print(f"1. Connecting to {HOST} as '{USERNAME}'...")
    try:
        ssh_key = paramiko.RSAKey.from_private_key_file(SSH_KEY_PATH)
    except Exception as e:
        print(f"❌ Failed to load SSH key: {e}")
        return

    transport = paramiko.Transport((HOST, 22))
    try:
        transport.connect(username=USERNAME, pkey=ssh_key)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print("✅ Successfully connected to SFTP server!")
        
        remote_files = sftp.listdir(REMOTE_DIR)
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        
        downloaded_zips = []
        
        # Download all .zip.pgp files
        for filename in remote_files:
            if filename.endswith('.zip.pgp'):
                remote_filepath = f"{REMOTE_DIR}/{filename}"
                local_filepath = os.path.join(DOWNLOAD_DIR, filename)
                print(f"   > Downloading {filename}...")
                sftp.get(remote_filepath, local_filepath)
                downloaded_zips.append(local_filepath)
                
    except Exception as e:
        print(f"❌ SFTP Error: {e}")
        return
    finally:
        if 'sftp' in locals(): sftp.close()
        transport.close()

    # --- DECRYPTION & UNZIPPING PHASE ---
    print("\n2. Decrypting and Unzipping Files...")
    try:
        # Be sure to keep the gpgbinary path you added earlier if it was required!
        gpg = gnupg.GPG(gpgbinary='/opt/homebrew/bin/gpg')
        
        with open(PGP_KEY_PATH, 'r') as key_file:
            gpg.import_keys(key_file.read())
            
        for enc_file in downloaded_zips:
            # Step A: Decrypt .zip.pgp to .zip
            base_name = os.path.basename(enc_file)
            decrypted_zip_name = base_name.replace('.pgp', '')
            local_zip_path = os.path.join(DOWNLOAD_DIR, decrypted_zip_name)
            
            with open(enc_file, 'rb') as f:
                status = gpg.decrypt_file(f, output=local_zip_path)
                
            if status.ok:
                print(f"✅ Decrypted to: {decrypted_zip_name}")
                
                # Step B: Unzip the file
                with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                    extracted_files = zip_ref.namelist()
                    zip_ref.extractall(DOWNLOAD_DIR)
                    print(f"   📂 Extracted contents from {decrypted_zip_name}:")
                    for extracted in extracted_files:
                        print(f"      - {extracted}")
                        
                # Optional: Delete the intermediate .zip file to keep things clean
                os.remove(local_zip_path)
            else:
                print(f"❌ Decryption failed for {base_name}: {status.status}")

    except Exception as e:
        print(f"❌ Processing Error: {e}")

if __name__ == "__main__":
    run_test()