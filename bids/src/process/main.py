#/BDSP/bids_app/src/process/main.py
import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# 전역 변수를 저장할 딕셔너리
global_vars = {}

def __init__():
    """초기화 함수"""
    logger.info("Process module initialized")

def load_json_config(json_file_path):
    """JSON 설정 파일 로드"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f"JSON 설정 파일 로드 성공: {json_file_path}")
        return config
    except Exception as e:
        logger.error(f"JSON 설정 파일 로드 실패: {e}")
        raise

def validate_and_initialize_config(config):
    """Step 0: JSON 변수 정리 및 초기화"""
    logger.info("Step 0: 설정 검증 및 초기화")
    
    try:
        # 필수 필드 확인 (uploadTime 추가)
        required_fields = ['user', 'systemId', 'projectCode', 'projectSeq', 'orgId', 'trialIndex', 'uploadTime','subjectId']
        missing_fields = []
        
        for field in required_fields:
            if field not in config:
                missing_fields.append(field)
        
        if missing_fields:
            logger.error(f"누락된 필수 필드: {missing_fields}")
            raise Exception(f"누락된 필수 필드: {missing_fields}")
                
        # 선택적 필드 초기화
        if 'domain' not in config or not config['domain']:
            config['domain'] = "DATA"
            print("Domain 필드를 기본값으로 초기화")
        
        # bodyPart 필드 초기화 (기본값: BRAIN)
        if 'bodyPart' not in config or not config['bodyPart']:
            config['bodyPart'] = "BRAIN"
            print("BodyPart 필드를 기본값으로 초기화: BRAIN")
        else:
            print(f"Body Part: {config['bodyPart']}")
        
        # category 필드 초기화 (기본값: IMAGE)
        if 'category' not in config or not config['category']:
            config['category'] = "IMAGE"
            print("Category 필드를 기본값으로 초기화: IMAGE")
        else:
            print(f"Category: {config['category']}")
        
        # public 필드 초기화 (기본값: False)
        if 'public' not in config or config['public'] is None:
            config['public'] = False
            print("Public 필드를 기본값으로 초기화: False")
        else:
            print(f"Public: {config['public']}")
        
        # task 필드 초기화 (기본값: isFunc: False, option: "")
        if 'task' not in config:
            config['task'] = {
                "isFunc": False,
                "option": ""
            }
            print("Task 설정을 기본값으로 초기화")
        else:
            # task 필드 검증
            task = config['task']
            if 'isFunc' not in task:
                task['isFunc'] = False
            if 'option' not in task:
                task['option'] = ""
            print(f"Task 설정 검증 완료 - isFunc: {task['isFunc']}")
        
        if 'flag' not in config:
            config['flag'] = []
            print("Flag 배열을 빈 배열로 초기화")
        else:
            # flag 배열 검증 및 초기화
            print(f"{len(config['flag'])}개의 flag 항목 발견")
            for i, flag_item in enumerate(config['flag']):
                if 'process' not in flag_item:
                    flag_item['process'] = ""
                if 'enabled' not in flag_item:
                    flag_item['enabled'] = False
                if 'options' not in flag_item:
                    flag_item['options'] = ""
                print(f"Flag {i+1}: {flag_item['process']} (활성화: {flag_item['enabled']})")
        
        # 프로젝트 시퀀스를 문자열로 변환 (경로 생성시 일관성 위해)
        config['projectSeq'] = str(config['projectSeq'])
        
        # structured_config 생성 - request 딕셔너리에 bodyPart, category, public 포함
        structured_config = {
            'request': {
                'user': config['user'],
                'systemId': config['systemId'],
                'projectCode': config['projectCode'],
                'projectSeq': config['projectSeq'],
                'orgId': config['orgId'],
                'trialIndex': config['trialIndex'],
                'uploadTime': config['uploadTime'],
                'domain': config['domain'],
                'bodyPart': config['bodyPart'],  
                'category': config['category'],
                'public': config['public']   
            },
            'task': config['task'],
            'flag': {
                'processes': config['flag'],
                'enabled_count': sum(1 for flag in config['flag'] if flag.get('enabled', False)),
                'enabled_processes': [flag['process'] for flag in config['flag'] if flag.get('enabled', False)]
            }
        }
        
        logger.info("설정 검증 및 초기화가 성공적으로 완료됨")
        print("모든 설정 변수가 검증 및 초기화됨")
        
        return structured_config
        
    except Exception as e:
        logger.error(f"설정 검증 및 초기화 실패: {e}")
        raise

def create_mis_structure(structured_config,global_vars):
    """Step 1: Medical Information System Structure 생성"""
    logger.info("Step 1: Medical Information System 구조 생성")
    try:
        request = structured_config['request']
        globals = global_vars
        print("로직 구성예정")
        mis_path = f"/data/{request['orgId']}/{request['projectCode']}"
        
        logger.info(f"MIS 구조 생성됨: {mis_path}")
        return mis_path
    except Exception as e:
        logger.error(f"MIS 구조 생성 실패: {e}")
        raise

def create_origindata_path(structured_config, base_path):
    """Step 2: origindata path 생성"""
    logger.info("Step 2: origindata 경로 생성")
    try:
        request = structured_config['request']
        print(f"user: {request['user']}, projectSeq: {request['projectSeq']}에 대한 origindata 경로 생성")
        origindata_path = f"{base_path}/origindata/{request['user']}/{request['projectSeq']}"
        
        logger.info(f"Origindata 경로 생성됨: {origindata_path}")
        return origindata_path
    except Exception as e:
        logger.error(f"Origindata 경로 생성 실패: {e}")
        raise

def create_sourcedata_path(structured_config, base_path):
    """Step 3: sourcedata path 생성"""
    logger.info("Step 3: sourcedata 경로 생성")
    try:
        request = structured_config['request']
        print(f"user: {request['user']}, projectSeq: {request['projectSeq']}에 대한 sourcedata 경로 생성")
        sourcedata_path = f"{base_path}/sourcedata/{request['user']}/{request['projectSeq']}"
        
        logger.info(f"Sourcedata 경로 생성됨: {sourcedata_path}")
        return sourcedata_path
    except Exception as e:
        logger.error(f"Sourcedata 경로 생성 실패: {e}")
        raise

def create_rawdata_path(structured_config, base_path):
    """Step 4: rawdata path 생성"""
    logger.info("Step 4: rawdata 경로 생성")
    try:
        request = structured_config['request']
        print(f"user: {request['user']}, projectSeq: {request['projectSeq']}에 대한 rawdata 경로 생성")
        rawdata_path = f"{base_path}/rawdata/{request['user']}/{request['projectSeq']}"
        
        logger.info(f"Rawdata 경로 생성됨: {rawdata_path}")
        return rawdata_path
    except Exception as e:
        logger.error(f"Rawdata 경로 생성 실패: {e}")
        raise

def create_thumbnail(structured_config, paths):
    """Step 5: thumbnail 생성"""
    logger.info("Step 5: 썸네일 생성")
    try:
        request = structured_config['request']
        print(f"user: {request['user']}, projectSeq: {request['projectSeq']}에 대한 썸네일 생성")
        thumbnail_path = f"{paths['base']}/thumbnails/{request['user']}/{request['projectSeq']}"
        
        logger.info(f"썸네일 생성됨: {thumbnail_path}")
        return thumbnail_path
    except Exception as e:
        logger.error(f"썸네일 생성 실패: {e}")
        raise

def export_json(structured_config, paths):
    """Step 6: export json 생성"""
    logger.info("Step 6: export json 생성")
    try:
        request = structured_config['request']
        print(f"user: {request['user']}, projectSeq: {request['projectSeq']}에 대한 export json 생성")
        export_path = f"{paths['base']}/exports/{request['user']}_{request['projectSeq']}_export.json"
        
        logger.info(f"Export json 생성됨: {export_path}")
        return export_path
    except Exception as e:
        logger.error(f"Export json 생성 실패: {e}")
        raise

def process_flags(structured_config, paths):
    """Step 7: process flag 처리 (조건부 실행)"""
    logger.info("Step 7: Flag 처리")
    
    flag = structured_config['flag']
    
    if flag['enabled_count'] == 0:
        logger.info("처리할 활성화된 flag가 없음")
        return
    
    try:
        print(f"{flag['enabled_count']}개의 활성화된 프로세스 처리: {', '.join(flag['enabled_processes'])}")
        
        for flag_item in flag['processes']:
            if flag_item.get('enabled', False):
                process_name = flag_item.get('process', '')
                options = flag_item.get('options', '')
                
                logger.info(f"Flag 처리: {process_name}, 옵션: {options}")
                print(f"{process_name} 프로세스 실행 중, 옵션: {options}")
                
                # 각 프로세스별 처리 로직
                if process_name == 'defacing':
                    print("defacing 알고리즘 실행 중...")
                elif process_name == 'canonical':
                    print("canonical orientation 프로세스 실행 중...")
                elif process_name == 'normalization':
                    print("normalization 프로세스 실행 중...")
                else:
                    print(f"알 수 없는 프로세스: {process_name}")
        
        logger.info("Flag 처리 완료")
    except Exception as e:
        logger.error(f"Step 7 - Flag 처리 실패: {e}")
        raise

def main(json_file_path, upload_dir=None, backup_dir=None, error_dir=None, working_dir=None,
         dicom_modality=None, nifti_modality=None, parrec_modality=None,
         flag_dir=None):
    """JSON 파일을 처리하는 메인 함수"""
    global global_vars
    
    __init__()
    
    # app.py에서 전달받은 모든 변수들을 global_vars 딕셔너리에 저장
    global_vars = {
        'json_file_path': json_file_path,
        'upload_dir': upload_dir,
        'backup_dir': backup_dir,
        'error_dir': error_dir,
        'working_dir': working_dir,
        'dicom_modality': dicom_modality,
        'nifti_modality': nifti_modality,
        'parrec_modality': parrec_modality,
        'flag_dir': flag_dir
    }
    
    logger.info(f"JSON 파일 처리 시작: {json_file_path}")
    logger.info(f"전역 변수 저장 완료: {len(global_vars)}개 변수")
    print(f"전역 변수들이 global_vars 딕셔너리에 저장됨:")
    for key, value in global_vars.items():
        if value is not None:
            print(f"  {key}: {value}")
    
    try:
        # JSON 설정 로드
        config = load_json_config(json_file_path)
        
        # Step 0: 변수 정리 및 초기화
        structured_config = validate_and_initialize_config(config)
        
        # Step 1: MIS 구조 생성
        base_path = create_mis_structure(structured_config,global_vars)
        
        # Step 2: origin 경로 생성
        origindata_path = create_origindata_path(structured_config, base_path)
        
        # Step 3: source 생성
        sourcedata_path = create_sourcedata_path(structured_config, base_path)
        
        # Step 4: raw 경로 생성
        rawdata_path = create_rawdata_path(structured_config, base_path)
        
        # 경로 정보 저장
        paths = {
            'base': base_path,
            'origindata': origindata_path,
            'sourcedata': sourcedata_path,
            'rawdata': rawdata_path
        }
        
        # Step 5: 썸네일 생성
        thumbnail_path = create_thumbnail(structured_config, paths)
        paths['thumbnail'] = thumbnail_path
        
        # Step 6: Export JSON 생성
        export_path = export_json(structured_config, paths)
        paths['export'] = export_path
        
        # Step 7: Process flags (조건부)
        process_flags(structured_config, paths)
        
        logger.info("모든 처리 단계가 성공적으로 완료됨")
        
    except Exception as e:
        logger.error(f"처리 실패: {e}")
        raise