import paramiko
import gnupg
import os
import stat
import zipfile

def fetch_files_via_sftp(host, username, ssh_key_path, ssh_public_key_path, remote_dir, local_download_dir):
    """Connects via SFTP using an SSH key pair and downloads all files."""
    print(f"   > Connecting to {host} via SFTP...")
    
    try:
        ssh_key = paramiko.RSAKey.from_private_key_file(ssh_key_path)
    except Exception as e:
        print(f"Error loading SSH private key: {e}")
        return []

    # Load and verify SSH public key matches private key
    try:
        with open(ssh_public_key_path, 'r') as pub_file:
            pub_key_data = pub_file.read().strip()
        pub_key_b64 = pub_key_data.split()[1] if len(pub_key_data.split()) >= 2 else ''
        if pub_key_b64 == ssh_key.get_base64():
            print("   > SSH key pair verified ✓")
        else:
            print("   > WARNING: SSH public key does not match private key!")
    except Exception as e:
        print(f"   > Warning: Could not verify SSH public key: {e}")

    transport = paramiko.Transport((host, 22))
    try:
        transport.connect(username=username, pkey=ssh_key)
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        os.makedirs(local_download_dir, exist_ok=True)
        
        # --- DIAGNOSTIC: Recurisvely explore the full SFTP directory tree ---
        print("\n   > === SFTP SERVER EXPLORATION ===")
        dirs_to_scan = ['.', remote_dir]
        scanned = set()
        all_remote_files = []  # (remote_path, filename) tuples
        
        for scan_dir in dirs_to_scan:
            if scan_dir in scanned:
                continue
            scanned.add(scan_dir)
            try:
                entries = sftp.listdir_attr(scan_dir)
                print(f"   > [{scan_dir}/] contains {len(entries)} item(s):")
                for entry in entries:
                    full_path = f"{scan_dir}/{entry.filename}" if scan_dir != '.' else entry.filename
                    # Check if it's a directory (S_ISDIR)
                    if stat.S_ISDIR(entry.st_mode):
                        print(f"   >   📁 {entry.filename}/")
                        # Add subdirectory for scanning
                        if full_path not in scanned:
                            dirs_to_scan.append(full_path)
                    else:
                        print(f"   >   📄 {entry.filename}  ({entry.st_size} bytes)")
                        all_remote_files.append((scan_dir, entry.filename))
            except Exception as e:
                print(f"   >   ⚠️  Cannot list {scan_dir}: {e}")
        
        print(f"\n   > Total files found across all directories: {len(all_remote_files)}")
        print("   > === END EXPLORATION ===\n")
        
        # --- Download ALL encrypted files from ALL directories ---
        downloaded_files = []
        for file_dir, filename in all_remote_files:
            if filename.lower().endswith(('.pgp', '.gpg')):
                remote_filepath = f"{file_dir}/{filename}"
                local_filepath = os.path.join(local_download_dir, filename)
                
                print(f"   > Downloading {remote_filepath}...")
                sftp.get(remote_filepath, local_filepath)
                downloaded_files.append(local_filepath)
            
        return downloaded_files
        
    finally:
        if 'sftp' in locals(): sftp.close()
        transport.close()


def decrypt_pgp_files(pgp_private_key_path, pgp_public_key_path, encrypted_files, output_dir, pgp_passphrase=None):
    """Decrypts downloaded .pgp files using full PGP key pair, unzips into organized subdirectories.
    
    Returns a dict: { 'inception': [csv_paths], 'quarterly': [csv_paths], 'other': [csv_paths] }
    """
    print("   > Decrypting and extracting files...")
    
    gpg = gnupg.GPG(gpgbinary='/opt/homebrew/bin/gpg')
    
    # Import BOTH the public and private PGP keys into the keyring
    with open(pgp_public_key_path, 'r') as pub_file:
        pub_result = gpg.import_keys(pub_file.read())
        if pub_result.count > 0:
            print(f"   > PGP public key imported ✓")
        else:
            print(f"   > WARNING: PGP public key import failed")
    
    with open(pgp_private_key_path, 'r') as priv_file:
        priv_result = gpg.import_keys(priv_file.read())
        if priv_result.count > 0:
            print(f"   > PGP private key imported ✓")
        else:
            print(f"   > WARNING: PGP private key import failed")
        
    os.makedirs(output_dir, exist_ok=True)
    results = {'inception': [], 'quarterly': [], 'other': []}
    
    for enc_file in encrypted_files:
        if not enc_file.endswith('.pgp') and not enc_file.endswith('.gpg'):
            continue 
            
        base_name = os.path.basename(enc_file)
        decrypted_name = base_name.replace('.pgp', '').replace('.gpg', '')
        temp_file_path = os.path.join(output_dir, decrypted_name)
        
        # 1. Decrypt
        with open(enc_file, 'rb') as f:
            status = gpg.decrypt_file(f, passphrase=pgp_passphrase, output=temp_file_path)
            
        if status.ok:
            # 2. Classify by keyword: inception or quarterly (zip and csv); everything else → other
            lower_name = decrypted_name.lower()
            if 'inception' in lower_name:
                category = 'inception'
            elif 'quarterly' in lower_name:
                category = 'quarterly'
            else:
                category = 'other'
            
            # 3. Extract zip files into a named subdirectory
            if decrypted_name.endswith('.zip'):
                zip_subdir_name = decrypted_name.replace('.zip', '')
                zip_subdir = os.path.join(output_dir, zip_subdir_name)
                os.makedirs(zip_subdir, exist_ok=True)
                
                with zipfile.ZipFile(temp_file_path, 'r') as zip_ref:
                    extracted_files = zip_ref.namelist()
                    zip_ref.extractall(zip_subdir)
                    for ext_file in extracted_files:
                        full_path = os.path.join(zip_subdir, ext_file)
                        print(f"   > Extracted [{category}]: {ext_file}")
                        results[category].append(full_path)
                
                os.remove(temp_file_path)
            else:
                print(f"   > Decrypted File [{category}]: {decrypted_name}")
                results[category].append(temp_file_path)
        else:
            print(f"   > Error decrypting {base_name}: {status.status}")
            
    return results