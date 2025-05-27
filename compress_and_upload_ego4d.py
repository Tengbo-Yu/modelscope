#!/usr/bin/env python3
import os
import tarfile
import subprocess
import shutil
from pathlib import Path
import logging
import math
import tempfile
import time
import json
import sys
from tqdm import tqdm

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_dir_size(path):
    """获取目录大小（以字节为单位）"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size

def get_all_files(input_path):
    """获取所有文件及其大小"""
    files = []
    for dirpath, dirnames, filenames in os.walk(input_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                files.append((fp, os.path.getsize(fp)))
    return files

def upload_to_modelscope(file_path, repo_name="Turboyuuuu/ego"):
    """上传文件到ModelScope"""
    try:
        cmd = f"modelscope upload {repo_name} {file_path} --repo-type dataset"
        logging.info(f"执行上传命令: {cmd}")
        result = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(f"上传成功: {file_path}")
        logging.debug(f"上传输出: {result.stdout.decode()}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"上传失败: {e}")
        logging.error(f"错误输出: {e.stderr.decode()}")
        return False

def save_progress(progress_file, current_archive, processed_files):
    """保存当前进度到文件"""
    with open(progress_file, 'w') as f:
        json.dump({
            'current_archive': current_archive,
            'processed_files': processed_files
        }, f)
    logging.info(f"进度已保存到 {progress_file}，当前归档编号: {current_archive}")

def load_progress(progress_file):
    """从文件加载进度"""
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                data = json.load(f)
            logging.info(f"从 {progress_file} 加载进度，继续从归档编号 {data['current_archive']} 开始")
            return data['current_archive'], set(data['processed_files'])
        except Exception as e:
            logging.error(f"加载进度文件失败: {e}")
    return 1, set()

def check_disk_space(path, required_gb=60):
    """检查指定路径的可用磁盘空间"""
    stat = os.statvfs(path)
    free_bytes = stat.f_frsize * stat.f_bavail
    free_gb = free_bytes / (1024**3)
    logging.info(f"当前可用磁盘空间: {free_gb:.2f} GB")
    return free_gb >= required_gb

def compress_upload_and_clean(input_dir, temp_dir, max_size_gb=50, repo_name="Turboyuuuu/ego", resume=True):
    """压缩目录，上传每个压缩文件，然后删除已上传的压缩文件"""
    input_path = Path(input_dir)
    temp_path = Path(temp_dir)
    temp_path.mkdir(parents=True, exist_ok=True)
    
    # 进度文件路径
    progress_file = temp_path / "upload_progress.json"
    
    # 将GB转换为字节
    max_size_bytes = max_size_gb * 1024 * 1024 * 1024
    
    # 获取所有文件
    logging.info("获取所有文件列表...")
    all_files = get_all_files(input_path)
    total_size = sum(size for _, size in all_files)
    total_files = len(all_files)
    logging.info(f"总文件数: {total_files}, 总文件大小: {total_size / (1024**3):.2f} GB")
    
    # 计算需要多少个压缩文件
    num_archives = math.ceil(total_size / max_size_bytes)
    logging.info(f"预计创建 {num_archives} 个压缩文件")
    
    # 加载进度（如果有）
    current_archive, processed_files_paths = load_progress(progress_file) if resume else (1, set())
    processed_files = set(processed_files_paths)
    logging.info(f"已处理 {len(processed_files)} 个文件, 当前归档编号: {current_archive}")
    
    # 创建压缩文件、上传并删除
    current_size = 0
    current_files = []
    
    # 创建进度条
    pbar = tqdm(total=total_files, desc="处理文件进度", unit="文件")
    pbar.update(len(processed_files))
    
    for file_path, file_size in all_files:
        # 跳过已处理的文件
        if file_path in processed_files:
            continue
            
        # 检查磁盘空间是否足够
        if not check_disk_space(temp_dir, required_gb=max_size_gb+10):
            pbar.close()
            logging.error(f"磁盘空间不足，需要至少 {max_size_gb+10} GB 可用空间")
            logging.info("暂停上传，请清理磁盘空间后重新运行脚本")
            save_progress(progress_file, current_archive, list(processed_files))
            return False
            
        # 当前压缩文件已满或这是一个很大的文件，创建新的压缩文件
        if (current_size + file_size > max_size_bytes and current_files) or file_size > max_size_bytes:
            if current_files:
                # 处理正常大小的文件集合
                archive_name = f"ego4d_part_{current_archive:03d}.tar.gz"
                archive_path = temp_path / archive_name
                
                logging.info(f"创建压缩文件 {archive_name} (包含 {len(current_files)} 个文件)")
                
                # 显示压缩进度
                with tqdm(total=len(current_files), desc=f"压缩 {archive_name}", unit="文件") as compress_pbar:
                    with tarfile.open(archive_path, "w:gz") as tar:
                        for f in current_files:
                            tar.add(f, arcname=os.path.relpath(f, input_path))
                            compress_pbar.update(1)
                
                # 上传压缩文件
                archive_size_mb = os.path.getsize(archive_path) / (1024 * 1024)
                logging.info(f"上传压缩文件 {archive_name} (大小: {archive_size_mb:.2f} MB)")
                
                # 上传和更新进度条
                upload_success = False
                with tqdm(total=100, desc=f"上传 {archive_name}", unit="%") as upload_pbar:
                    # 由于modelscope没有提供进度反馈，我们模拟一个上传进度
                    upload_pbar.update(5)  # 立即显示5%进度
                    start_time = time.time()
                    upload_success = upload_to_modelscope(archive_path, repo_name)
                    upload_duration = time.time() - start_time
                    upload_pbar.update(95)  # 完成剩余进度
                
                if upload_success:
                    # 上传成功后删除压缩文件
                    logging.info(f"删除已上传的压缩文件 {archive_name}")
                    os.remove(archive_path)
                    # 标记这些文件为已处理
                    processed_files.update(current_files)
                    pbar.update(len(current_files))
                    # 保存进度
                    save_progress(progress_file, current_archive + 1, list(processed_files))
                    # 显示上传速度
                    upload_speed = archive_size_mb / upload_duration if upload_duration > 0 else 0
                    logging.info(f"上传速度: {upload_speed:.2f} MB/s")
                else:
                    pbar.close()
                    logging.error(f"上传失败，保留压缩文件 {archive_name} 以便稍后重试")
                    save_progress(progress_file, current_archive, list(processed_files))
                    return False
                
                current_archive += 1
                current_size = 0
                current_files = []
            
            # 处理超大文件（大于max_size_bytes的单个文件）
            if file_size > max_size_bytes:
                logging.warning(f"文件 {file_path} 超过大小限制 ({file_size / (1024**3):.2f} GB > {max_size_gb} GB)")
                logging.info(f"单独处理大文件: {file_path}")
                
                # 对于超大文件，可以选择单独处理或跳过
                # 这里选择单独压缩并上传
                big_file_archive = f"ego4d_bigfile_{current_archive:03d}.tar.gz"
                big_file_path = temp_path / big_file_archive
                
                # 显示压缩进度
                with tqdm(total=1, desc=f"压缩大文件 {big_file_archive}", unit="文件") as big_file_pbar:
                    with tarfile.open(big_file_path, "w:gz") as tar:
                        tar.add(file_path, arcname=os.path.relpath(file_path, input_path))
                        big_file_pbar.update(1)
                
                # 上传大文件
                big_file_size_mb = os.path.getsize(big_file_path) / (1024 * 1024)
                with tqdm(total=100, desc=f"上传大文件 {big_file_archive}", unit="%") as big_upload_pbar:
                    big_upload_pbar.update(5)  # 立即显示5%进度
                    start_time = time.time()
                    upload_success = upload_to_modelscope(big_file_path, repo_name)
                    upload_duration = time.time() - start_time
                    big_upload_pbar.update(95)  # 完成剩余进度
                
                if upload_success:
                    os.remove(big_file_path)
                    processed_files.add(file_path)
                    pbar.update(1)
                    save_progress(progress_file, current_archive + 1, list(processed_files))
                    # 显示上传速度
                    upload_speed = big_file_size_mb / upload_duration if upload_duration > 0 else 0
                    logging.info(f"大文件上传速度: {upload_speed:.2f} MB/s")
                else:
                    pbar.close()
                    logging.error(f"上传大文件失败，保留压缩文件 {big_file_archive} 以便稍后重试")
                    save_progress(progress_file, current_archive, list(processed_files))
                    return False
                
                current_archive += 1
                continue
        
        current_files.append(file_path)
        current_size += file_size
    
    # 处理最后一批文件
    if current_files:
        archive_name = f"ego4d_part_{current_archive:03d}.tar.gz"
        archive_path = temp_path / archive_name
        logging.info(f"创建最后一个压缩文件 {archive_name} (包含 {len(current_files)} 个文件)")
        
        # 显示压缩进度
        with tqdm(total=len(current_files), desc=f"压缩 {archive_name}", unit="文件") as compress_pbar:
            with tarfile.open(archive_path, "w:gz") as tar:
                for f in current_files:
                    tar.add(f, arcname=os.path.relpath(f, input_path))
                    compress_pbar.update(1)
        
        # 上传最后一个压缩文件
        archive_size_mb = os.path.getsize(archive_path) / (1024 * 1024)
        logging.info(f"上传最后一个压缩文件 {archive_name} (大小: {archive_size_mb:.2f} MB)")
        
        # 上传和更新进度条
        with tqdm(total=100, desc=f"上传 {archive_name}", unit="%") as upload_pbar:
            upload_pbar.update(5)  # 立即显示5%进度
            start_time = time.time()
            upload_success = upload_to_modelscope(archive_path, repo_name)
            upload_duration = time.time() - start_time
            upload_pbar.update(95)  # 完成剩余进度
        
        if upload_success:
            logging.info(f"删除已上传的最后一个压缩文件 {archive_name}")
            os.remove(archive_path)
            processed_files.update(current_files)
            pbar.update(len(current_files))
            # 显示上传速度
            upload_speed = archive_size_mb / upload_duration if upload_duration > 0 else 0
            logging.info(f"上传速度: {upload_speed:.2f} MB/s")
        else:
            pbar.close()
            logging.error(f"上传失败，保留最后一个压缩文件 {archive_name} 以便稍后重试")
            save_progress(progress_file, current_archive, list(processed_files))
            return False
    
    pbar.close()
    
    # 所有文件处理完成，删除进度文件
    if os.path.exists(progress_file):
        os.remove(progress_file)
        logging.info("所有文件上传完成，已删除进度文件")
    
    return True

if __name__ == "__main__":
    input_directory = "/nvmessd/ssd_share/tengbo/Ego4d/data/ego4d/v2/full_scale"
    temp_directory = "/nvmessd/ssd_share/tengbo/Ego4d/data/ego4d_compressed_temp"
    repo_name = "Turboyuuuu/ego"
    
    # 确保临时目录存在
    os.makedirs(temp_directory, exist_ok=True)
    
    # 记录开始时间
    start_time = time.time()
    
    logging.info("开始压缩和上传过程...")
    try:
        success = compress_upload_and_clean(input_directory, temp_directory, max_size_gb=50, repo_name=repo_name, resume=True)
        
        # 记录总耗时
        elapsed_time = time.time() - start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if success:
            logging.info(f"压缩和上传过程完成！总耗时: {int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒")
        else:
            logging.warning(f"压缩和上传过程中断！已运行时间: {int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒")
            logging.info("请解决问题后重新运行脚本，将从中断处继续")
    except KeyboardInterrupt:
        # 处理Ctrl+C中断
        logging.warning("用户中断操作，保存当前进度...")
        elapsed_time = time.time() - start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        logging.warning(f"已运行时间: {int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒")
        logging.info("请重新运行脚本，将从中断处继续") 