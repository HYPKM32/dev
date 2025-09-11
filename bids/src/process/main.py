#/BDSP/bids_app/src/process/main.py
import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

def __init__():
    """초기화 함수"""
    logger.info("Process module initialized")

def load_json_config(json_file_path):
    """JSON 설정 파일 로드"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.info(f"Successfully loaded JSON config: {json_file_path}")
        return config
    except Exception as e:
        logger.error(f"Failed to load JSON config: {e}")
        raise

def validate_and_initialize_config(config):
    """Step 0: JSON 변수 정리 및 초기화"""
    logger.info("Step 0: Validating and initializing configuration")
    
    try:
        # 필수 필드 확인
        required_fields = ['user', 'systemId', 'projectCode', 'projectSeq', 'orgId', 'trialIndex']
        missing_fields = []
        
        for field in required_fields:
            if field not in config:
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # 변수 타입 검증 및 초기화
        print(f"Validating config for user: {config['user']}")
        print(f"System ID: {config['systemId']}")
        print(f"Project Code: {config['projectCode']}")
        print(f"Project Sequence: {config['projectSeq']}")
        print(f"Organization ID: {config['orgId']}")
        print(f"Trial Index: {config['trialIndex']}")
        
        # 선택적 필드 초기화
        if 'uploadTime' not in config or not config['uploadTime']:
            config['uploadTime'] = "00000000"
            print("Upload time initialized to default value")
        
        if 'domain' not in config:
            config['domain'] = "DATA"
            print("Domain field initialized to empty string")
        
        # bodyPart 필드 초기화
        if 'bodyPart' not in config:
            config['bodyPart'] = "BRAIN"
            print("BodyPart field initialized to empty string")
        else:
            print(f"Body Part: {config['bodyPart']}")
        
        # category 필드 초기화
        if 'category' not in config:
            config['category'] = "IMAGE"
            print("Category field initialized to default value: IMAGE")
        else:
            print(f"Category: {config['category']}")
        
        if 'task' not in config:
            config['task'] = {
                "hasTask": False,
                "taskType": "",
                "taskName": "",
                "options": ""
            }
            print("Task configuration initialized with default values")
        else:
            # task 필드 검증
            task = config['task']
            if 'hasTask' not in task:
                task['hasTask'] = False
            if 'taskType' not in task:
                task['taskType'] = ""
            if 'taskName' not in task:
                task['taskName'] = ""
            if 'options' not in task:
                task['options'] = ""
            print(f"Task configuration validated - hasTask: {task['hasTask']}")
        
        if 'flag' not in config:
            config['flag'] = []
            print("Flag array initialized as empty")
        else:
            # flag 배열 검증 및 초기화
            print(f"Found {len(config['flag'])} flag entries")
            for i, flag_item in enumerate(config['flag']):
                if 'process' not in flag_item:
                    flag_item['process'] = ""
                if 'enabled' not in flag_item:
                    flag_item['enabled'] = False
                if 'options' not in flag_item:
                    flag_item['options'] = ""
                print(f"Flag {i+1}: {flag_item['process']} (enabled: {flag_item['enabled']})")
        
        # 프로젝트 시퀀스를 문자열로 변환 (경로 생성시 일관성 위해)
        config['projectSeq'] = str(config['projectSeq'])
        
        # structured_config 생성 - request 딕셔너리에 bodyPart, category 포함
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
                'bodyPart': config['bodyPart'],  # bodyPart 추가
                'category': config['category']   # category 추가
            },
            'task': config['task'],
            'flag': {
                'processes': config['flag'],
                'enabled_count': sum(1 for flag in config['flag'] if flag.get('enabled', False)),
                'enabled_processes': [flag['process'] for flag in config['flag'] if flag.get('enabled', False)]
            }
        }
        
        logger.info("Configuration validation and initialization completed successfully")
        print("All configuration variables validated and initialized")
        print(f"Body Part added to request: {structured_config['request']['bodyPart']}")
        print(f"Category added to request: {structured_config['request']['category']}")
        
        return structured_config
        
    except Exception as e:
        logger.error(f"Failed to validate and initialize config: {e}")
        raise

def create_mis_structure(structured_config):
    """Step 1: Medical Information System Structure 생성"""
    logger.info("Step 1: Creating Medical Information System Structure")
    try:
        request = structured_config['request']
        print(f"Creating MIS structure for orgId: {request['orgId']}, projectCode: {request['projectCode']}")
        base_path = f"/data/{request['orgId']}/{request['projectCode']}"
        
        logger.info(f"MIS structure created at: {base_path}")
        return base_path
    except Exception as e:
        logger.error(f"Failed to create MIS structure: {e}")
        raise

def create_origindata_path(structured_config, base_path):
    """Step 2: origindata path 생성"""
    logger.info("Step 2: Creating origindata path")
    try:
        request = structured_config['request']
        print(f"Creating origindata path for user: {request['user']}, projectSeq: {request['projectSeq']}")
        origindata_path = f"{base_path}/origindata/{request['user']}/{request['projectSeq']}"
        
        logger.info(f"Origindata path created: {origindata_path}")
        return origindata_path
    except Exception as e:
        logger.error(f"Failed to create origindata path: {e}")
        raise

def create_sourcedata_path(structured_config, base_path):
    """Step 3: sourcedata path 생성"""
    logger.info("Step 3: Creating sourcedata path")
    try:
        request = structured_config['request']
        print(f"Creating sourcedata path for user: {request['user']}, projectSeq: {request['projectSeq']}")
        sourcedata_path = f"{base_path}/sourcedata/{request['user']}/{request['projectSeq']}"
        
        logger.info(f"Sourcedata path created: {sourcedata_path}")
        return sourcedata_path
    except Exception as e:
        logger.error(f"Failed to create sourcedata path: {e}")
        raise

def create_rawdata_path(structured_config, base_path):
    """Step 4: rawdata path 생성"""
    logger.info("Step 4: Creating rawdata path")
    try:
        request = structured_config['request']
        print(f"Creating rawdata path for user: {request['user']}, projectSeq: {request['projectSeq']}")
        rawdata_path = f"{base_path}/rawdata/{request['user']}/{request['projectSeq']}"
        
        logger.info(f"Rawdata path created: {rawdata_path}")
        return rawdata_path
    except Exception as e:
        logger.error(f"Failed to create rawdata path: {e}")
        raise

def create_thumbnail(structured_config, paths):
    """Step 5: thumbnail 생성"""
    logger.info("Step 5: Creating thumbnail")
    try:
        request = structured_config['request']
        print(f"Creating thumbnail for user: {request['user']}, projectSeq: {request['projectSeq']}")
        thumbnail_path = f"{paths['base']}/thumbnails/{request['user']}/{request['projectSeq']}"
        
        logger.info(f"Thumbnail created at: {thumbnail_path}")
        return thumbnail_path
    except Exception as e:
        logger.error(f"Failed to create thumbnail: {e}")
        raise

def export_json(structured_config, paths):
    """Step 6: export json 생성"""
    logger.info("Step 6: Creating export json")
    try:
        request = structured_config['request']
        print(f"Creating export json for user: {request['user']}, projectSeq: {request['projectSeq']}")
        export_path = f"{paths['base']}/exports/{request['user']}_{request['projectSeq']}_export.json"
        
        logger.info(f"Export json created: {export_path}")
        return export_path
    except Exception as e:
        logger.error(f"Failed to create export json: {e}")
        raise

def process_flags(structured_config, paths):
    """Step 7: process flag 처리 (조건부 실행)"""
    logger.info("Step 7: Processing flags")
    
    flag = structured_config['flag']
    
    if flag['enabled_count'] == 0:
        logger.info("No flags enabled for processing")
        return
    
    try:
        print(f"Processing {flag['enabled_count']} enabled processes: {', '.join(flag['enabled_processes'])}")
        
        for flag_item in flag['processes']:
            if flag_item.get('enabled', False):
                process_name = flag_item.get('process', '')
                options = flag_item.get('options', '')
                
                logger.info(f"Processing flag: {process_name} with options: {options}")
                print(f"Executing {process_name} process with options: {options}")
                
                # 각 프로세스별 처리 로직
                if process_name == 'defacing':
                    print("Running defacing algorithm...")
                elif process_name == 'canonical':
                    print("Running canonical orientation process...")
                elif process_name == 'normalization':
                    print("Running normalization process...")
                else:
                    print(f"Unknown process: {process_name}")
        
        logger.info("Flag processing completed")
    except Exception as e:
        logger.error(f"Failed to process flags: {e}")
        raise

def main(json_file_path):
    """JSON 파일을 처리하는 메인 함수"""
    __init__()
    
    logger.info(f"Starting processing for JSON file: {json_file_path}")
    
    try:
        # JSON 설정 로드
        config = load_json_config(json_file_path)
        
        # Step 0: 변수 정리 및 초기화
        structured_config = validate_and_initialize_config(config)
        
        # Step 1: MIS 구조 생성
        base_path = create_mis_structure(structured_config)
        
        # Step 2-4: 각종 경로 생성
        origindata_path = create_origindata_path(structured_config, base_path)
        sourcedata_path = create_sourcedata_path(structured_config, base_path)
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
        
        logger.info("All processing steps completed successfully")
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise