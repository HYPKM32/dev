import cv2
import nibabel as nib
import sys
import numpy as np
import os
from pathlib import Path
from utils.common import bdsp_walk


def create_thumbnail(nii_path, output_path):
    """개별 NIfTI 파일에 대한 썸네일 생성"""
    try:
        img_data = nib.load(nii_path).get_fdata()
        if len(img_data.shape) > 3:
            img_data = img_data[:,:,:,1]

        # 3개 방향의 중간 슬라이스 추출
        img1 = img_data[:,:,int(img_data.shape[2]/2)]  # axial
        img2 = img_data[:,int(img_data.shape[1]/2),:]  # coronal  
        img3 = img_data[int(img_data.shape[0]/2),:,:]  # sagittal
        
        # 144x144로 리사이즈
        img1 = cv2.resize(img1, (144,144))
        img2 = cv2.resize(img2, (144,144))
        img3 = cv2.resize(img3, (144,144))
        
        # 3개 이미지를 세로로 합치고 전처리
        timg = np.transpose(np.concatenate((img1,img2,img3), axis=0))
        timg = timg[::-1]
        timg = timg/float(np.max(timg))*255
        
        # CLAHE 적용
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
        timg = clahe.apply(timg.astype('uint8'))
        
        # 썸네일 저장
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        cv2.imwrite(output_path, timg)
        
        return True
    except Exception as e:
        print(f"Error creating thumbnail for {nii_path}: {e}")
        return False

def thumbnail(raw_path):
    """
    raw_path 딕셔너리를 받아서 각 nii.gz 파일의 썸네일을 생성
    
    Args:
        raw_path (dict): {source_path: nii_file_path} 형태의 딕셔너리
        
    Returns:
        dict: {nii_file_path: thumbnail_path} 형태의 딕셔너리
    """
    thumbnail_path = {}
    
    for source_path, nii_file_path in raw_path.items():
        try:
            # nii.gz 파일 경로에서 썸네일 경로 생성
            nii_path = Path(nii_file_path)
            
            # rawdata를 thumbnail로 변경하고 확장자를 .png로 변경
            thumbnail_dir = str(nii_path.parent)
            thumbnail_filename = nii_path.stem.replace('.nii', '') + '.png'
            thumb_path = os.path.join(thumbnail_dir, thumbnail_filename)
            
            # 썸네일 생성
            if create_thumbnail(nii_file_path, thumb_path):
                thumbnail_path[nii_file_path] = thumb_path
                bdsp_walk(thumbnail_dir)
                print(f"썸네일 생성 완료: {thumb_path}")
            else:
                print(f"썸네일 생성 실패: {nii_file_path}")
                
        except Exception as e:
            print(f"Error processing {nii_file_path}: {e}")
    
    return thumbnail_path

if __name__ == "__main__":
    print("This module should be imported and used via thumbnail() function")