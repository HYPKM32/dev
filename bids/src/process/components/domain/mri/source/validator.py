#/BDSP/bids_app/src/process/components/domain/mri/source/validation.py
import json
import hashlib
import logging
from pathlib import Path
import pydicom
from pydicom.errors import InvalidDicomError
import shutil
import os
import re
from collections import defaultdict
import nibabel as nib
import numpy as np
from typing import List, Tuple

logger = logging.getLogger(__name__)


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


def validate_parrec_files(par_path):
    """
    PAR/REC 파일 유효성 검사 함수
    
    Args:
        par_path (str): PAR 파일 경로
    
    Returns:
        dict: 검사 결과
    """
    results = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'success': [],
        'info': {}
    }
    
    # REC 파일 경로 생성
    rec_path = par_path.replace('.par', '.rec')
    
    try:
        # 1. 파일 존재 확인
        if not os.path.exists(par_path):
            results['errors'].append(f"PAR 파일이 존재하지 않음: {par_path}")
            results['valid'] = False
            return results
        else:
            results['success'].append("✓ PAR 파일 존재 확인")
            
        if not os.path.exists(rec_path):
            results['errors'].append(f"REC 파일이 존재하지 않음: {rec_path}")
            results['valid'] = False
            return results
        else:
            results['success'].append("✓ REC 파일 존재 확인")
        
        # 2. PAR 파일 파싱
        header_info = parse_par_header(par_path)
        if not header_info:
            results['errors'].append("PAR 파일 헤더 파싱 실패")
            results['valid'] = False
            return results
        else:
            results['success'].append("✓ PAR 파일 헤더 파싱 성공")
        
        results['info'].update(header_info)
        
        # 3. IMAGE INFORMATION 섹션 파싱
        image_info = parse_image_information(par_path)
        if not image_info:
            results['errors'].append("IMAGE INFORMATION 섹션 파싱 실패")
            results['valid'] = False
            return results
        else:
            results['success'].append("✓ IMAGE INFORMATION 섹션 파싱 성공")
        
        # 4. 기본 유효성 검사들
        check_header_validity(header_info, results)
        check_data_completeness(header_info, image_info, results)
        check_file_size_consistency(header_info, image_info, rec_path, results)
        check_index_continuity(image_info, results)
        check_metadata_consistency(image_info, results)
        
        # 5. 최종 결과
        if results['errors']:
            results['valid'] = False
        
        return results
        
    except Exception as e:
        results['errors'].append(f"검사 중 오류 발생: {str(e)}")
        results['valid'] = False
        return results


def parse_par_header(par_path):
    """PAR 파일 헤더 정보 추출"""
    header_info = {}
    
    with open(par_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            
            # 헤더 섹션 종료 확인
            if line.startswith("#  sl ec  dyn"):
                break
                
            # 주요 매개변수 추출
            if "Scan resolution" in line:
                match = re.search(r':\s*(\d+)\s+(\d+)', line)
                if match:
                    header_info['scan_resolution_x'] = int(match.group(1))
                    header_info['scan_resolution_y'] = int(match.group(2))
            
            elif "Max. number of slices/locations" in line:
                match = re.search(r':\s*(\d+)', line)
                if match:
                    header_info['max_slices'] = int(match.group(1))
            
            elif "Max. number of dynamics" in line:
                match = re.search(r':\s*(\d+)', line)
                if match:
                    header_info['max_dynamics'] = int(match.group(1))
            
            elif "Max. number of echoes" in line:
                match = re.search(r':\s*(\d+)', line)
                if match:
                    header_info['max_echoes'] = int(match.group(1))
            
            elif "Max. number of cardiac phases" in line:
                match = re.search(r':\s*(\d+)', line)
                if match:
                    header_info['max_phases'] = int(match.group(1))
    
    return header_info


def parse_image_information(par_path):
    """IMAGE INFORMATION 섹션 파싱"""
    image_data = []
    
    with open(par_path, 'r', encoding='utf-8', errors='ignore') as f:
        in_data_section = False
        
        for line in f:
            line = line.strip()
            
            # 데이터 섹션 시작 확인
            if line.startswith("#  sl ec  dyn"):
                in_data_section = True
                continue
            
            # 데이터 라인 처리
            if in_data_section and line and not line.startswith("#"):
                parts = line.split()
                if len(parts) >= 10:  # 최소 필수 컬럼 수
                    try:
                        image_data.append({
                            'slice': int(parts[0]),
                            'echo': int(parts[1]),
                            'dynamic': int(parts[2]),
                            'phase': int(parts[3]),
                            'type': int(parts[4]),
                            'sequence': int(parts[5]),
                            'index': int(parts[6]),
                            'bit_depth': int(parts[7]),
                            'scan_percent': int(parts[8]),
                            'recon_x': int(parts[9]),
                            'recon_y': int(parts[10]) if len(parts) > 10 else int(parts[9])
                        })
                    except (ValueError, IndexError):
                        continue
    
    return image_data


def check_header_validity(header_info, results):
    """헤더 유효성 검사"""
    required_fields = ['max_slices', 'max_dynamics', 'max_echoes', 'max_phases']
    
    header_errors = []
    for field in required_fields:
        if field not in header_info:
            header_errors.append(f"필수 헤더 정보 누락: {field}")
        elif header_info[field] <= 0:
            header_errors.append(f"잘못된 {field} 값: {header_info[field]}")
    
    if header_errors:
        results['errors'].extend(header_errors)
    else:
        results['success'].append("✓ 헤더 필수 정보 모두 유효")


def check_data_completeness(header_info, image_info, results):
    """데이터 완전성 검사"""
    if not image_info:
        results['errors'].append("IMAGE INFORMATION 데이터가 없음")
        return
    
    # 예상 총 이미지 수
    expected_total = (header_info.get('max_slices', 0) * 
                     header_info.get('max_dynamics', 0) * 
                     header_info.get('max_echoes', 1) * 
                     header_info.get('max_phases', 1))
    
    actual_total = len(image_info)
    results['info']['expected_images'] = expected_total
    results['info']['actual_images'] = actual_total
    
    if actual_total != expected_total:
        results['errors'].append(f"이미지 수 불일치: 예상 {expected_total}, 실제 {actual_total}")
    else:
        results['success'].append(f"✓ 이미지 수 일치: {actual_total}개")
    
    # 각 차원별 범위 확인
    slices = set(img['slice'] for img in image_info)
    dynamics = set(img['dynamic'] for img in image_info)
    echoes = set(img['echo'] for img in image_info)
    phases = set(img['phase'] for img in image_info)
    
    results['info']['slice_range'] = f"{min(slices)}-{max(slices)} ({len(slices)}개)"
    results['info']['dynamic_range'] = f"{min(dynamics)}-{max(dynamics)} ({len(dynamics)}개)"
    
    # 누락된 조합 확인
    expected_combinations = set()
    for s in range(1, header_info.get('max_slices', 0) + 1):
        for d in range(1, header_info.get('max_dynamics', 0) + 1):
            for e in range(1, header_info.get('max_echoes', 1) + 1):
                for p in range(1, header_info.get('max_phases', 1) + 1):
                    expected_combinations.add((s, d, e, p))
    
    actual_combinations = set((img['slice'], img['dynamic'], img['echo'], img['phase']) 
                             for img in image_info)
    
    missing = expected_combinations - actual_combinations
    if missing:
        results['warnings'].append(f"누락된 이미지 조합 {len(missing)}개")
    else:
        results['success'].append("✓ 모든 슬라이스-다이나믹스 조합 완전")


def check_file_size_consistency(header_info, image_info, rec_path, results):
    """파일 크기 일관성 검사"""
    if not image_info:
        return
    
    # 첫 번째 이미지에서 매개변수 추출
    first_img = image_info[0]
    bit_depth = first_img['bit_depth']
    recon_x = first_img['recon_x']
    recon_y = first_img['recon_y']
    
    # 예상 REC 파일 크기 계산
    bytes_per_pixel = bit_depth // 8
    total_images = len(image_info)
    expected_size = recon_x * recon_y * total_images * bytes_per_pixel
    
    # 실제 파일 크기
    actual_size = os.path.getsize(rec_path)
    
    results['info']['expected_rec_size'] = expected_size
    results['info']['actual_rec_size'] = actual_size
    results['info']['size_match'] = actual_size == expected_size
    
    if actual_size != expected_size:
        results['errors'].append(f"REC 파일 크기 불일치: 예상 {expected_size}, 실제 {actual_size}")
    else:
        results['success'].append(f"✓ REC 파일 크기 일치: {actual_size:,} bytes")


def check_index_continuity(image_info, results):
    """인덱스 연속성 검사"""
    indices = [img['index'] for img in image_info]
    indices.sort()
    
    expected_indices = list(range(len(image_info)))
    
    if indices != expected_indices:
        results['warnings'].append("인덱스가 연속적이지 않음")
    else:
        results['success'].append("✓ 인덱스 연속성 확인")
    
    results['info']['index_range'] = f"{min(indices)}-{max(indices)}"


def check_metadata_consistency(image_info, results):
    """메타데이터 일관성 검사"""
    if not image_info:
        return
    
    # 모든 이미지가 같은 속성을 가져야 하는 필드들
    consistent_fields = ['bit_depth', 'recon_x', 'recon_y']
    
    inconsistent_fields = []
    for field in consistent_fields:
        values = set(img[field] for img in image_info)
        if len(values) > 1:
            inconsistent_fields.append(f"{field} 값이 일관되지 않음: {values}")
        else:
            results['info'][f'{field}_value'] = list(values)[0]
    
    if inconsistent_fields:
        results['warnings'].extend(inconsistent_fields)
    else:
        results['success'].append("✓ 모든 이미지 메타데이터 일관성 확인")


def validate_nifti_file(nifti_path):
    """
    NIfTI 파일 유효성 검사 함수
    
    Args:
        nifti_path (str): NIfTI 파일 경로
    
    Returns:
        dict: 검사 결과
    """
    results = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'success': [],
        'info': {}
    }
    
    try:
        # 1. 파일 존재 확인
        if not os.path.exists(nifti_path):
            results['errors'].append(f"NIfTI 파일이 존재하지 않음: {nifti_path}")
            results['valid'] = False
            return results
        else:
            results['success'].append("✓ NIfTI 파일 존재 확인")
        
        # 2. 파일 로드 시도
        img = nib.load(nifti_path)
        results['success'].append("✓ NIfTI 파일 로드 성공")
        
        # 3. 기본 정보 추출
        shape = img.shape
        data_dtype = img.get_data_dtype()
        
        results['info']['shape'] = shape
        results['info']['data_type'] = str(data_dtype)
        results['info']['dimensions'] = len(shape)
        
        # 4. 헤더 정보 검사
        header = img.header
        results['info']['header_valid'] = True
        
        # 복셀 크기 정보
        try:
            pixdim = header.get_zooms()
            results['info']['voxel_size'] = pixdim[:3]
            results['success'].append("✓ 복셀 크기 정보 추출 성공")
        except Exception as e:
            results['warnings'].append(f"복셀 크기 정보 추출 실패: {e}")
        
        # 공간 정보 (affine matrix)
        try:
            affine = img.affine
            det = np.linalg.det(affine[:3, :3])
            results['info']['affine_determinant'] = det
            
            if abs(det) < 1e-10:
                results['warnings'].append("변환 행렬의 determinant가 0에 가까움")
            else:
                results['success'].append("✓ 공간 변환 행렬 정상")
        except Exception as e:
            results['warnings'].append(f"공간 정보 검사 실패: {e}")
        
        # 5. 데이터 유효성 검사
        try:
            data = img.get_fdata()
            
            # NaN 값 확인
            nan_count = np.isnan(data).sum()
            if nan_count > 0:
                results['warnings'].append(f"NaN 값 {nan_count}개 발견")
            else:
                results['success'].append("✓ NaN 값 없음")
            
            # 무한값 확인
            inf_count = np.isinf(data).sum()
            if inf_count > 0:
                results['warnings'].append(f"무한값 {inf_count}개 발견")
            else:
                results['success'].append("✓ 무한값 없음")
            
            # 데이터 통계
            results['info']['data_range'] = [float(np.min(data)), float(np.max(data))]
            results['info']['non_zero_voxels'] = int(np.count_nonzero(data))
            results['info']['total_voxels'] = int(data.size)
            
            results['success'].append("✓ 데이터 접근 및 기본 통계 계산 성공")
            
        except Exception as e:
            results['errors'].append(f"데이터 접근 실패: {e}")
            results['valid'] = False
        
        # 6. 차원 유효성 검사
        if len(shape) < 3:
            results['errors'].append(f"잘못된 차원: {len(shape)}D (최소 3D 필요)")
            results['valid'] = False
        elif len(shape) > 4:
            results['warnings'].append(f"비정상적인 고차원: {len(shape)}D")
        else:
            results['success'].append(f"✓ 적절한 차원: {len(shape)}D")
        
        # 7. 파일 크기 일관성 검사
        try:
            expected_size = np.prod(shape) * np.dtype(data_dtype).itemsize
            actual_size = os.path.getsize(nifti_path)
            
            # .nii.gz 파일은 압축되어 있으므로 크기가 다를 수 있음
            if nifti_path.endswith('.gz'):
                results['info']['compressed'] = True
                results['success'].append("✓ 압축 파일 확인")
            else:
                results['info']['compressed'] = False
                # 비압축 파일의 경우 대략적인 크기 검사
                if actual_size < expected_size * 0.5:  # 너무 작으면 문제
                    results['warnings'].append("파일 크기가 예상보다 현저히 작음")
        except Exception as e:
            results['warnings'].append(f"파일 크기 검사 실패: {e}")
        
        return results
        
    except Exception as e:
        results['errors'].append(f"NIfTI 파일 검사 중 오류 발생: {str(e)}")
        results['valid'] = False
        return results

class DicomValidator:
    def __init__(self, invalid_data_path: str | Path, valid_data_path: str | Path):
        self.invalid_data_path = Path(invalid_data_path)
        self.valid_data_path = Path(valid_data_path)

    def run(self) -> List[Tuple[str, str]] | None:
        jpath = self.invalid_data_path / "bdsp_file_list.json"
        if not jpath.exists():
            logger.error(f"{jpath} not found")
            return None
        try:
            data = json.loads(jpath.read_text(encoding="utf-8"))
        except Exception as e:
            logger.error(f"Failed to read {jpath}: {e}")
            return None

        # (StudyUID, SeriesUID) -> [Path...]
        groups: dict[tuple[str, str], list[Path]] = {}

        for item in data.get("path", []):
            p = Path(item.get("file_path", ""))
            cur = self.invalid_data_path / p.name
            if not cur.exists():
                logger.error(f"File not found: {cur}")
                continue
            try:
                ds = pydicom.dcmread(str(cur), stop_before_pixels=True, force=True)
            except (InvalidDicomError, Exception):
                logger.info(f"Skipping non-DICOM file: {cur.name}")
                continue

            study_uid = getattr(ds, "StudyInstanceUID", None)
            series_uid = getattr(ds, "SeriesInstanceUID", None)
            if not study_uid or not series_uid:
                logger.warning(f"Missing StudyUID or SeriesUID in {cur}")
                continue

            groups.setdefault((study_uid, series_uid), []).append(cur)

        if not groups:
            logger.error("No valid DICOM files found")
            return None

        results: List[Tuple[str, str]] = []
        for (study_uid, series_uid), files in groups.items():
            set_id = "set-" + _sha1(f"{study_uid}|{series_uid}")
            target_dir = self.valid_data_path / set_id
            target_dir.mkdir(parents=True, exist_ok=True)

            for src in files:
                dst = target_dir / src.name
                try:
                    src.rename(dst)
                except OSError:
                    shutil.move(str(src), str(dst))

            logger.info(f"[DICOM] Validation group → {target_dir} (files: {len(files)})")
            results.append((str(target_dir), set_id))

        return results


class ParrecValidator:
    def __init__(self, invalid_data_path: str | Path, valid_data_path: str | Path):
        self.invalid_data_path = Path(invalid_data_path)
        self.valid_data_path = Path(valid_data_path)

    def run(self) -> List[Tuple[str, str]] | None:
        jpath = self.invalid_data_path / "bdsp_file_list.json"
        if not jpath.exists():
            logger.error(f"{jpath} not found")
            return None
        try:
            data = json.loads(jpath.read_text(encoding="utf-8"))
        except Exception as e:
            logger.error(f"Failed to read {jpath}: {e}")
            return None

        # stem -> {'par': Path|None, 'rec': Path|None}
        pairs: dict[str, dict[str, Path | None]] = {}

        for item in data.get("path", []):
            p = Path(item.get("file_path", ""))
            cur = self.invalid_data_path / p.name
            if not cur.exists():
                logger.error(f"File not found: {cur}")
                continue

            suf = cur.suffix.lower()
            if suf == '.par' or suf == '.rec':
                info = pairs.setdefault(cur.stem, {'par': None, 'rec': None})
                if suf == '.par':
                    info['par'] = cur
                else:
                    info['rec'] = cur

        results: List[Tuple[str, str]] = []

        for stem, pr in pairs.items():
            par_file, rec_file = pr['par'], pr['rec']
            if not par_file or not rec_file:
                logger.error(f"Missing PAR/REC pair for stem '{stem}'")
                continue

            # 개별 페어 단위로 유효성 검사
            res = validate_parrec_files(str(par_file))
            if not res.get('valid'):
                logger.error(f"✗ PAR/REC validation failed: {par_file.name} / {rec_file.name}")
                continue

            set_id = "set-" + _sha1(f"{par_file.name}|{rec_file.name}")
            target_dir = self.valid_data_path / set_id
            target_dir.mkdir(parents=True, exist_ok=True)

            for src in (par_file, rec_file):
                dst = target_dir / src.name
                try:
                    src.rename(dst)
                except OSError:
                    shutil.move(str(src), str(dst))

            logger.info(f"[PAR/REC] Validation pair → {target_dir} ({par_file.name}, {rec_file.name})")
            results.append((str(target_dir), set_id))

        if not results:
            logger.error("No valid PAR/REC pairs found")
            return None
        return results
    



class NiftiValidator:
    def __init__(self, invalid_data_path: str | Path, valid_data_path: str | Path):
        self.invalid_data_path = Path(invalid_data_path)
        self.valid_data_path = Path(valid_data_path)

    def run(self) -> List[Tuple[str, str]] | None:
        jpath = self.invalid_data_path / "bdsp_file_list.json"
        if not jpath.exists():
            logger.error(f"{jpath} not found")
            return None
        try:
            data = json.loads(jpath.read_text(encoding="utf-8"))
        except Exception as e:
            logger.error(f"Failed to read {jpath}: {e}")
            return None

        # stem -> [Path...(.nii / .nii.gz)]
        groups: dict[str, list[Path]] = {}

        for item in data.get("path", []):
            p = Path(item.get("file_path", ""))
            cur = self.invalid_data_path / p.name
            if not cur.exists():
                logger.error(f"File not found: {cur}")
                continue
            name_low = cur.name.lower()
            if cur.suffix.lower() == '.nii' or name_low.endswith('.nii.gz'):
                groups.setdefault(cur.stem, []).append(cur)

        if not groups:
            logger.error("No NIfTI files found")
            return None

        results: List[Tuple[str, str]] = []

        for stem, files in groups.items():
            # 파일들 전부 유효성 검사 (하나라도 invalid면 해당 그룹 skip)
            all_valid = True
            for nf in files:
                vr = validate_nifti_file(str(nf))
                if not vr.get('valid'):
                    logger.error(f"✗ NIfTI validation failed: {nf.name}")
                    all_valid = False
                    break
            if not all_valid:
                continue

            # set_id: stem 기반 (간단하고 안정적)
            fnames = "|".join(sorted(f.name for f in files))
            set_id = "set-" + _sha1(f"{stem}|{fnames}")

            target_dir = self.valid_data_path / set_id
            target_dir.mkdir(parents=True, exist_ok=True)

            for src in files:
                dst = target_dir / src.name
                try:
                    src.rename(dst)
                except OSError:
                    shutil.move(str(src), str(dst))

            logger.info(f"[NIfTI] Validation group → {target_dir} (files: {len(files)})")
            results.append((str(target_dir), set_id))

        if not results:
            logger.error("No valid NIfTI sets produced")
            return None
        return results