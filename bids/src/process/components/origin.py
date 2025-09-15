import os
import shutil
import logging
import zipfile
from pathlib import Path
from utils import common

logger = logging.getLogger(__name__)

def clean_system_files(directory):
    """
    시스템에서 자동으로 생성되는 불필요한 파일들 제거
    
    Args:
        directory: 정리할 디렉토리 경로
    """
    unwanted_patterns = [
        '.DS_Store',      # macOS 폴더 설정
        '._*',            # macOS 리소스 포크 파일
        '__MACOSX',       # macOS zip 메타데이터 폴더
        '.localized',     # macOS 현지화 파일
        'Thumbs.db',      # Windows 썸네일 캐시
        'desktop.ini',    # Windows 폴더 설정
        '~$*',            # Office 임시 잠금 파일
        '*.tmp'           # 임시 파일
    ]
    
    removed_count = 0
    directory_path = Path(directory)
    
    for pattern in unwanted_patterns:
        try:
            # glob으로 패턴에 맞는 파일/폴더 찾기
            for item in directory_path.rglob(pattern):
                try:
                    if item.is_file():
                        item.unlink()
                        print(f"  불필요한 파일 제거: {item.relative_to(directory_path)}")
                        removed_count += 1
                    elif item.is_dir():
                        shutil.rmtree(item)
                        print(f"  불필요한 폴더 제거: {item.relative_to(directory_path)}")
                        removed_count += 1
                except Exception as e:
                    logger.warning(f"파일 제거 실패: {item} - {e}")
        except Exception as e:
            logger.warning(f"패턴 '{pattern}' 검색 실패: {e}")
    
    if removed_count > 0:
        print(f"총 {removed_count}개의 불필요한 파일/폴더가 제거되었습니다.")
    else:
        print("제거할 불필요한 파일이 없습니다.")

def create_origin_path(structured_config, global_vars, mss_path):
    """
    Origin 경로 생성 및 zip 파일 처리
    
    Args:
        structured_config: 구조화된 설정 정보
        global_vars: 전역 변수 딕셔너리
        mss_path: MSS 기본 경로
    
    Returns:
        str: 생성된 origin 경로
    """
    logger.info("Step 2: Origin 경로 생성 시작")
    
    try:
        # 1. structured_config의 request 딕셔너리에서 entity 추출
        request = structured_config['request']
        upload_dir = global_vars['upload_dir']
        user = request['user']
        upload_time = request['uploadTime']
        
        if not user or not upload_time:
            raise ValueError("User 또는 UploadTime 정보가 없습니다")
        
        print(f"User: {user}, UploadTime: {upload_time}")
        
        # 2. upload_dir/user/upload_time 하위의 모든 파일 찾기 및 zip 파일 체크
        src_dir = Path(upload_dir) / user / upload_time
        
        if not src_dir.exists():
            raise FileNotFoundError(f"업로드 디렉토리에 이미지가 존재하지 않습니다: {src_dir}")
        
        # 해당 디렉토리의 모든 파일 찾기
        all_files = list(src_dir.glob("*"))
        
        # zip 파일만 필터링
        zip_files = [f for f in all_files if f.is_file() and f.suffix.lower() == '.zip']
        
        if not zip_files:
            logger.error(f"zip 파일이 없습니다: {src_dir}")
            raise FileNotFoundError(f"zip 파일이 없습니다: {src_dir}")
        
        if len(all_files) == 0:
            logger.error(f"빈 폴더입니다: {src_dir}")
            raise FileNotFoundError(f"빈 폴더입니다: {src_dir}")
        
        print(f"{len(zip_files)}개의 zip 파일 발견:")
        for zip_file in zip_files:
            print(f"  - {zip_file.name}")
        
        # 4. originpath 생성: mss_path/origin/user/upload_time
        origin_path = Path(mss_path) / "origin" / user / upload_time
        
        # 디렉토리 생성
        origin_path.mkdir(parents=True, exist_ok=True)
        print(f"Origin 경로 생성: {origin_path}")
        
        # 5. zip 폴더와 unzip 폴더 생성
        zip_folder = origin_path / "zip"
        unzip_folder = origin_path / "unzip"
        
        zip_folder.mkdir(exist_ok=True)
        unzip_folder.mkdir(exist_ok=True)
        
        print(f"  - zip 폴더: {zip_folder}")
        print(f"  - unzip 폴더: {unzip_folder}")
        
        # 6. zip 파일들을 zip 폴더로 복사 및 압축 해제
        for zip_file in zip_files:
            destination = zip_folder / zip_file.name
            
            try:
                # zip 파일을 zip 폴더로 복사 (원본 유지)
                shutil.copy2(str(zip_file), str(destination))
                print(f"파일 복사: {zip_file.name} -> {zip_folder}")
                
                # zip 파일을 unzip 폴더로 압축 해제
                with zipfile.ZipFile(str(destination), 'r') as zip_ref:
                    zip_ref.extractall(str(unzip_folder))
                    print(f"압축 해제: {zip_file.name} -> {unzip_folder}")
                
                # 압축 해제 직후 불필요한 시스템 파일들 제거
                print(f"시스템 파일 정리 중...")
                clean_system_files(str(unzip_folder))
                
            except zipfile.BadZipFile as e:
                logger.error(f"손상된 zip 파일: {zip_file.name} - {e}")
                raise Exception(f"손상된 zip 파일: {zip_file.name} - {e}")
            except Exception as e:
                logger.error(f"파일 처리 실패: {zip_file.name} - {e}")
                raise Exception(f"파일 처리 실패: {zip_file.name} - {e}")
        
        # 7. zip 폴더와 unzip 폴더에 대해 파일 리스트 생성
        try:
            # zip 폴더 스캔 (zip 폴더 하위에 bdsp_file_list.json 생성)
            zip_list_file = zip_folder / "bdsp_file_list.json"
            common.bdsp_walk(str(zip_folder), str(zip_list_file))
            print(f"zip 폴더 파일 리스트 생성: {zip_list_file}")
            
            # unzip 폴더 스캔 (unzip 폴더 하위에 bdsp_file_list.json 생성)
            unzip_list_file = unzip_folder / "bdsp_file_list.json"
            common.bdsp_walk(str(unzip_folder), str(unzip_list_file))
            print(f"unzip 폴더 파일 리스트 생성: {unzip_list_file}")
            
        except Exception as e:
            logger.error(f"파일 리스트 생성 실패: {e}")
            raise Exception(f"파일 리스트 생성 실패: {e}")
        
        print(f"Origin 경로 생성 완료: zip 파일 처리 및 파일 리스트 생성됨")
        logger.info("Step 2 origin path making 완료")
        
        return str(origin_path)
        
    except Exception as e:
        logger.error(f"Step 2 origin path making 실패: {e}")
        raise