# /BDSP/bids_app/src/process/components/domain/mri/separation.py
import json
import logging
from pathlib import Path
import gzip
import shutil

logger = logging.getLogger(__name__)


class PreWork:
    """모든 Separator 클래스의 공통 부모 클래스"""
    
    def __init__(self, validated_dir: str, set_id: str = None):
        self.validated_dir = Path(validated_dir)
        self.set_id = set_id

    def _load_json_index(self, json_path: Path):
        """bdsp_file_list.json에서 파일 목록(index 순서대로) 로드"""
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return sorted(data.get("path", []), key=lambda x: x.get("index", 0))
        except Exception as e:
            logger.error(f"JSON index 로드 실패: {json_path} ({e})")
            return []


class DicomSeparator(PreWork):
    def run(self, validated_dir: str = None, set_id: str = None) -> str:
        """
        validated_dir 안의 DICOM 파일들을 JSON index 기준으로 재명명(rename).
        
        Args:
            validated_dir: validation을 통과한 디렉토리 (옵션, 생성자에서 설정 가능)
            set_id: 세트 ID (옵션, 생성자에서 설정 가능)
            
        Returns:
            str: 분리 결과가 저장된 최종 디렉토리 경로
        """
        # 파라미터 우선순위: 메서드 인자 > 생성자 인자
        work_dir = Path(validated_dir) if validated_dir else self.validated_dir
        work_set_id = set_id if set_id is not None else self.set_id
        
        if not work_set_id:
            raise ValueError("set_id는 필수 파라미터입니다.")

        json_path = work_dir / "bdsp_file_list.json"

        if not json_path.exists():
            raise FileNotFoundError(f"bdsp_file_list.json 없음: {json_path}")

        file_list = self._load_json_index(json_path)
        if not file_list:
            raise RuntimeError("JSON index에서 파일을 불러오지 못했습니다.")

        # 파일 이동 및 이름 변경 (같은 디렉토리 내에서)
        for item in file_list:
            src_path = work_dir / Path(item.get("file_path")).name  # 파일명만 사용
            if not src_path.exists():
                logger.warning(f"원본 파일 없음: {src_path}")
                continue

            index = item.get("index")
            ext = src_path.suffix  # 원래 확장자 유지
            new_name = f"item_{work_set_id}_{index:04d}{ext}"  # 0-padding 추가
            dst_path = work_dir / new_name

            try:
                src_path.rename(dst_path)
                logger.info(f"[DICOM 분리] {src_path} → {dst_path}")
            except Exception as e:
                logger.error(f"파일 이동 실패: {src_path} → {dst_path} ({e})")

        logger.info(f"DICOM 분리 완료: {work_dir}")
        return str(work_dir)


class ParrecSeparator(PreWork):
    def _get_parrec_number(self, file_path: Path) -> str:
        """PAR/REC 파일 타입에 따라 번호 할당
        
        Args:
            file_path: 파일 경로
            
        Returns:
            str: PAR 파일이면 "01", REC 파일이면 "02"
        """
        ext = file_path.suffix.lower()
        if ext == '.par':
            return "0001"
        elif ext == '.rec':
            return "0002"
        else:
            # 기본값으로 파일 확장자 기반 처리
            logger.warning(f"알 수 없는 확장자: {ext}")
            return "0000"
    
    def run(self, validated_dir: str = None, set_id: str = None) -> str:
        """PAR/REC 파일 분리
        
        Args:
            validated_dir: validation을 통과한 디렉토리 (옵션, 생성자에서 설정 가능)
            set_id: 세트 ID (옵션, 생성자에서 설정 가능)
            
        Returns:
            str: 분리 결과가 저장된 최종 디렉토리 경로
        """
        # 파라미터 우선순위: 메서드 인자 > 생성자 인자
        work_dir = Path(validated_dir) if validated_dir else self.validated_dir
        work_set_id = set_id if set_id is not None else self.set_id
        
        if not work_set_id:
            raise ValueError("set_id는 필수 파라미터입니다.")

        json_path = work_dir / "bdsp_file_list.json"

        if not json_path.exists():
            raise FileNotFoundError(f"bdsp_file_list.json 없음: {json_path}")

        file_list = self._load_json_index(json_path)
        if not file_list:
            raise RuntimeError("JSON index에서 파일을 불러오지 못했습니다.")

        # PAR/REC 파일들을 그룹핑하여 처리
        parrec_pairs = {}  # 기본 파일명 -> [par_path, rec_path]
        
        # 먼저 PAR/REC 쌍을 찾기
        for item in file_list:
            src_path = work_dir / Path(item.get("file_path")).name
            if not src_path.exists():
                logger.warning(f"원본 파일 없음: {src_path}")
                continue
                
            ext = src_path.suffix.lower()
            if ext in ['.par', '.rec']:
                # 확장자를 제거한 기본 파일명
                base_name = src_path.stem
                
                if base_name not in parrec_pairs:
                    parrec_pairs[base_name] = {'par': None, 'rec': None, 'index': item.get("index", 0)}
                
                if ext == '.par':
                    parrec_pairs[base_name]['par'] = src_path
                elif ext == '.rec':
                    parrec_pairs[base_name]['rec'] = src_path

        # PAR/REC 쌍별로 파일명 변경
        renamed_count = 0
        for base_name, pair_info in parrec_pairs.items():
            par_path = pair_info['par']
            rec_path = pair_info['rec']
            base_index = pair_info['index']
            
            # PAR 파일 처리
            if par_path and par_path.exists():
                par_number = self._get_parrec_number(par_path)
                new_par_name = f"item_{work_set_id}_{par_number}.par"
                dst_par_path = work_dir / new_par_name
                
                try:
                    par_path.rename(dst_par_path)
                    logger.info(f"[PAR/REC 분리] {par_path} → {dst_par_path}")
                    renamed_count += 1
                except Exception as e:
                    logger.error(f"PAR 파일 이동 실패: {par_path} → {dst_par_path} ({e})")
            
            # REC 파일 처리
            if rec_path and rec_path.exists():
                rec_number = self._get_parrec_number(rec_path)
                new_rec_name = f"item_{work_set_id}_{rec_number}.rec"
                dst_rec_path = work_dir / new_rec_name
                
                try:
                    rec_path.rename(dst_rec_path)
                    logger.info(f"[PAR/REC 분리] {rec_path} → {dst_rec_path}")
                    renamed_count += 1
                except Exception as e:
                    logger.error(f"REC 파일 이동 실패: {rec_path} → {dst_rec_path} ({e})")
        
        logger.info(f"PAR/REC 분리 완료: {work_dir} ({renamed_count}개 파일 처리)")
        return str(work_dir)


class NiftiSeparator(PreWork):
    def run(self, validated_dir: str = None, set_id: str = None) -> str:
        """NIfTI 파일 분리
        
        NIfTI 파일은 무조건 한 개씩만 들어오므로 index가 1이어야 함.
        index가 2 이상이면 오류로 처리.
        
        Args:
            validated_dir: validation을 통과한 디렉토리 (옵션, 생성자에서 설정 가능)
            set_id: 세트 ID (옵션, 생성자에서 설정 가능)
            
        Returns:
            str: 분리 결과가 저장된 최종 디렉토리 경로
        """
        # 파라미터 우선순위: 메서드 인자 > 생성자 인자
        work_dir = Path(validated_dir) if validated_dir else self.validated_dir
        work_set_id = set_id if set_id is not None else self.set_id
        
        if not work_set_id:
            raise ValueError("set_id는 필수 파라미터입니다.")

        json_path = work_dir / "bdsp_file_list.json"

        if not json_path.exists():
            raise FileNotFoundError(f"bdsp_file_list.json 없음: {json_path}")

        file_list = self._load_json_index(json_path)
        if not file_list:
            raise RuntimeError("JSON index에서 파일을 불러오지 못했습니다.")

        # NIfTI는 파일이 1개만 있어야 함
        if len(file_list) > 1:
            raise RuntimeError(f"NIfTI는 파일이 1개만 있어야 합니다. 현재 {len(file_list)}개가 있습니다.")
        
        if len(file_list) == 0:
            raise RuntimeError("처리할 NIfTI 파일이 없습니다.")

        # 단일 파일 처리
        item = file_list[0]
        index = item.get("index", 0)
        
        # index가 2 이상이면 문제상황
        if index >= 2:
            raise RuntimeError(f"NIfTI 파일의 index는 1이어야 합니다. 현재 {index}입니다. zip데이터에 문제가 있을 수 있습니다.")
        
        src_path = work_dir / Path(item.get("file_path")).name
        if not src_path.exists():
            raise FileNotFoundError(f"원본 파일 없음: {src_path}")

        if src_path.name.lower().endswith('.nii.gz'):
            ext = '.nii.gz'
            stem = src_path.name[:-len('.nii.gz')]
        else:
            ext = src_path.suffix
            stem = src_path.stem

        # suffix(모달리티) 추출: 파일명에서 마지막 '_' 뒤 토큰
        if '_' in stem:
            suffix = stem.rsplit('_', 1)[1]
        else:
            suffix = 'NIFTI'  # 기본값

        new_name = f"item_{work_set_id}_{suffix}_0001{ext}"
        dst_path = work_dir / new_name

        try:
            src_path.rename(dst_path)
            logger.info(f"[NIfTI 분리] {src_path} → {dst_path}")
        except Exception as e:
            logger.error(f"파일 이동 실패: {src_path} → {dst_path} ({e})")
            raise

        logger.info(f"NIfTI 분리 완료: {work_dir}")
        return str(work_dir)