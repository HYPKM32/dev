import os
from pathlib import Path

def check_byproduct(raw_path):
    """
    BIDS rawdata에서 .nii.gz와 .json 외의 부산물 파일들을 찾아 정리합니다.
    
    Args:
        raw_path (dict): {source_path: nifti_file_path} 형태의 딕셔너리
        
    Returns:
        dict: {nifti_file_path: {확장자: 파일경로}} 형태의 딕셔너리
        예시: {
            '/path/to/file.nii.gz': {
                'bval': '/path/to/file.bval',
                'bvec': '/path/to/file.bvec'
            }
        }
    """
    byproduct_results = {}
    
    # raw_path의 value 값들을 순회
    for source_path, nifti_path in raw_path.items():
        
        # 해당 NIfTI 파일의 기본 경로와 파일명 추출
        nifti_pathobj = Path(nifti_path)
        base_dir = nifti_pathobj.parent
        base_filename = nifti_pathobj.stem.replace('.nii', '')  # .nii.gz에서 .nii 제거
        
        # 같은 디렉토리에서 같은 base_filename을 가진 파일들 찾기
        byproduct_files = {}
        
        if os.path.exists(base_dir):
            for file in os.listdir(base_dir):
                file_path = os.path.join(base_dir, file)
                
                # 디렉토리는 제외
                if os.path.isdir(file_path):
                    continue
                
                # 현재 파일이 같은 base_filename으로 시작하는지 확인
                if file.startswith(base_filename):
                    file_pathobj = Path(file_path)
                    
                    # .nii.gz와 .json 파일은 제외
                    if file.endswith('.nii.gz') or file.endswith('.json'):
                        continue
                    
                    # 확장자 추출 (점 제거)
                    suffix = file_pathobj.suffix.lstrip('.')
                    
                    if suffix:  # 확장자가 있는 경우만
                        byproduct_files[suffix] = file_path
        
        # 결과 저장 (부산물이 있는 경우만)
        if byproduct_files:
            byproduct_results[nifti_path] = byproduct_files
    
    return byproduct_results