from typing import Dict, List, Set


FILTER_FIELDS = {
    "date": {"field_name": "WORK_DT", "description": "작업일자 (YYYYMMDD 형식)"},
    "process": {"field_name": "OPER_NAME", "description": "제조 공정명 또는 공정 그룹"},
    "oper_num": {"field_name": "OPER_NUM", "description": "공정번호. 공정과 매핑되는 4자리 숫자 코드"},
    "pkg_type1": {"field_name": "PKG_TYPE1", "description": "패키지 기술 유형 (예: FCBGA, LFBGA)"},
    "pkg_type2": {"field_name": "PKG_TYPE2", "description": "스택 수/구성 유형 (예: ODP, 16DP, SDP)"},
    "mode": {"field_name": "MODE", "description": "제품 모드 (예: DDR4, DDR5, LPDDR5)"},
    "den": {"field_name": "DEN", "description": "제품 용량/Density (예: 256G, 512G, 1T)"},
    "tech": {"field_name": "TECH", "description": "기술 유형 (예: LC, FO, FC)"},
    "lead": {"field_name": "LEAD", "description": "Ball 또는 Lead의 개수"},
    "mcp_no": {"field_name": "MCP_NO", "description": "MCP 제품 코드"},
}


PROCESS_GROUPS = {
    "DP": {
        "group_name": "DP",
        "synonyms": ["DP", "D/P"],
        "actual_values": [
            "WET1",
            "WET2",
            "L/T1",
            "L/T2",
            "B/G1",
            "B/G2",
            "H/S1",
            "H/S2",
            "W/S1",
            "W/S2",
            "WSD1",
            "WSD2",
            "WEC1",
            "WEC2",
            "WLS1",
            "WLS2",
            "WVI",
            "UV",
            "C/C1",
        ],
        "description": "전공정 DP 그룹",
    },
    "WET": {
        "group_name": "WET",
        "synonyms": ["WET"],
        "actual_values": ["WET1", "WET2"],
        "description": "WET 세부 공정 그룹",
    },
    "LT": {
        "group_name": "LT",
        "synonyms": ["LT", "L/T"],
        "actual_values": ["L/T1", "L/T2"],
        "description": "L/T 세부 공정 그룹",
    },
    "BG": {
        "group_name": "BG",
        "synonyms": ["BG", "B/G"],
        "actual_values": ["B/G1", "B/G2"],
        "description": "B/G 세부 공정 그룹",
    },
    "HS": {
        "group_name": "HS",
        "synonyms": ["HS", "H/S"],
        "actual_values": ["H/S1", "H/S2"],
        "description": "H/S 세부 공정 그룹",
    },
    "WS": {
        "group_name": "WS",
        "synonyms": ["WS", "W/S"],
        "actual_values": ["W/S1", "W/S2"],
        "description": "W/S 세부 공정 그룹",
    },
    "DA": {
        "group_name": "D/A",
        "synonyms": ["D/A", "DA", "Die Attach", "DIE ATTACH", "다이어태치", "다이본딩"],
        "actual_values": ["D/A1", "D/A2", "D/A3", "D/A4", "D/A5", "D/A6"],
        "description": "Die Attach 공정 그룹",
    },
    "PCO": {
        "group_name": "PCO",
        "synonyms": ["PCO"],
        "actual_values": ["PCO1", "PCO2", "PCO3", "PCO4", "PCO5", "PCO6"],
        "description": "PCO 공정 그룹",
    },
    "DC": {
        "group_name": "D/C",
        "synonyms": ["D/C", "DC"],
        "actual_values": ["D/C1", "D/C2", "D/C3", "D/C4"],
        "description": "D/C 공정 그룹",
    },
    "DI": {
        "group_name": "D/I",
        "synonyms": ["D/I", "DI"],
        "actual_values": ["D/I"],
        "description": "D/I 단일 공정",
    },
    "DS": {
        "group_name": "D/S",
        "synonyms": ["D/S", "DS"],
        "actual_values": ["D/S1"],
        "description": "D/S 공정 그룹",
    },
    "FCB": {
        "group_name": "FCB",
        "synonyms": ["FCB", "Flip Chip", "플립칩"],
        "actual_values": ["FCB1", "FCB2", "FCB/H"],
        "description": "FCB 공정 그룹",
    },
    "FCBH": {
        "group_name": "FCB/H",
        "synonyms": ["FCB/H", "FCBH"],
        "actual_values": ["FCB/H"],
        "description": "FCB/H 단일 공정",
    },
    "BM": {
        "group_name": "B/M",
        "synonyms": ["B/M", "BN", "비엠"],
        "actual_values": ["B/M"],
        "description": "B/M 단일 공정",
    },
    "PC": {
        "group_name": "P/C",
        "synonyms": ["P/C", "PC"],
        "actual_values": ["P/C1", "P/C2", "P/C3", "P/C4", "P/C5"],
        "description": "P/C 공정 그룹",
    },
    "WB": {
        "group_name": "W/B",
        "synonyms": ["W/B", "WB", "Wire Bonding", "와이어본딩"],
        "actual_values": ["W/B1", "W/B2", "W/B3", "W/B4", "W/B5", "W/B6"],
        "description": "Wire Bonding 공정 그룹",
    },
    "QCSPC": {
        "group_name": "QCSPC",
        "synonyms": ["QCSPC"],
        "actual_values": ["QCSPC1", "QCSPC2", "QCSPC3", "QCSPC4"],
        "description": "QCSPC 공정 그룹",
    },
    "SAT": {
        "group_name": "SAT",
        "synonyms": ["SAT"],
        "actual_values": ["SAT1", "SAT2"],
        "description": "SAT 공정 그룹",
    },
    "PL": {
        "group_name": "P/L",
        "synonyms": ["P/L", "PL"],
        "actual_values": ["PLH"],
        "description": "P/L 단일 공정",
    },
}


LITERAL_PROCESSES = [
    "WVI",
    "DVI",
    "BBMS",
    "AVI",
    "MDVI",
    "MDTI",
    "QCSAT",
    "LMDI",
    "DIC",
    "EVI",
]


def _dedupe_processes() -> List[str]:
    ordered: List[str] = []
    for group in PROCESS_GROUPS.values():
        for process_name in group["actual_values"]:
            if process_name not in ordered:
                ordered.append(process_name)
    for process_name in LITERAL_PROCESSES:
        if process_name not in ordered:
            ordered.append(process_name)
    return ordered


INDIVIDUAL_PROCESSES = _dedupe_processes()


PROCESS_SPECS = [
    {"family": "DP", "OPER_NAME": "WET1", "라인": "DP-L1", "OPER_NUM": "1000"},
    {"family": "DP", "OPER_NAME": "WET2", "라인": "DP-L1", "OPER_NUM": "1005"},
    {"family": "DP", "OPER_NAME": "L/T1", "라인": "DP-L2", "OPER_NUM": "1010"},
    {"family": "DP", "OPER_NAME": "L/T2", "라인": "DP-L2", "OPER_NUM": "1015"},
    {"family": "DP", "OPER_NAME": "B/G1", "라인": "DP-L3", "OPER_NUM": "1020"},
    {"family": "DP", "OPER_NAME": "B/G2", "라인": "DP-L3", "OPER_NUM": "1025"},
    {"family": "DP", "OPER_NAME": "H/S1", "라인": "DP-L4", "OPER_NUM": "1030"},
    {"family": "DP", "OPER_NAME": "H/S2", "라인": "DP-L4", "OPER_NUM": "1035"},
    {"family": "DP", "OPER_NAME": "W/S1", "라인": "DP-L5", "OPER_NUM": "1040"},
    {"family": "DP", "OPER_NAME": "W/S2", "라인": "DP-L5", "OPER_NUM": "1045"},
    {"family": "DP", "OPER_NAME": "WSD1", "라인": "DP-L6", "OPER_NUM": "1050"},
    {"family": "DP", "OPER_NAME": "WSD2", "라인": "DP-L6", "OPER_NUM": "1055"},
    {"family": "DP", "OPER_NAME": "WEC1", "라인": "DP-L7", "OPER_NUM": "1060"},
    {"family": "DP", "OPER_NAME": "WEC2", "라인": "DP-L7", "OPER_NUM": "1065"},
    {"family": "DP", "OPER_NAME": "WLS1", "라인": "DP-L8", "OPER_NUM": "1070"},
    {"family": "DP", "OPER_NAME": "WLS2", "라인": "DP-L8", "OPER_NUM": "1075"},
    {"family": "DP", "OPER_NAME": "WVI", "라인": "DP-L9", "OPER_NUM": "1080"},
    {"family": "DP", "OPER_NAME": "UV", "라인": "DP-L9", "OPER_NUM": "1085"},
    {"family": "DP", "OPER_NAME": "C/C1", "라인": "DP-L9", "OPER_NUM": "1090"},
    {"family": "DA", "OPER_NAME": "D/A1", "라인": "DA-L1", "OPER_NUM": "2000"},
    {"family": "DA", "OPER_NAME": "D/A2", "라인": "DA-L1", "OPER_NUM": "2010"},
    {"family": "DA", "OPER_NAME": "D/A3", "라인": "DA-L2", "OPER_NUM": "2020"},
    {"family": "DA", "OPER_NAME": "D/A4", "라인": "DA-L2", "OPER_NUM": "2030"},
    {"family": "DA", "OPER_NAME": "D/A5", "라인": "DA-L3", "OPER_NUM": "2040"},
    {"family": "DA", "OPER_NAME": "D/A6", "라인": "DA-L3", "OPER_NUM": "2050"},
    {"family": "PCO", "OPER_NAME": "PCO1", "라인": "PCO-L1", "OPER_NUM": "2100"},
    {"family": "PCO", "OPER_NAME": "PCO2", "라인": "PCO-L1", "OPER_NUM": "2110"},
    {"family": "PCO", "OPER_NAME": "PCO3", "라인": "PCO-L2", "OPER_NUM": "2120"},
    {"family": "PCO", "OPER_NAME": "PCO4", "라인": "PCO-L2", "OPER_NUM": "2130"},
    {"family": "PCO", "OPER_NAME": "PCO5", "라인": "PCO-L3", "OPER_NUM": "2140"},
    {"family": "PCO", "OPER_NAME": "PCO6", "라인": "PCO-L3", "OPER_NUM": "2150"},
    {"family": "DC", "OPER_NAME": "D/C1", "라인": "DC-L1", "OPER_NUM": "2200"},
    {"family": "DC", "OPER_NAME": "D/C2", "라인": "DC-L1", "OPER_NUM": "2210"},
    {"family": "DC", "OPER_NAME": "D/C3", "라인": "DC-L2", "OPER_NUM": "2220"},
    {"family": "DC", "OPER_NAME": "D/C4", "라인": "DC-L2", "OPER_NUM": "2230"},
    {"family": "DI", "OPER_NAME": "D/I", "라인": "DI-L1", "OPER_NUM": "2300"},
    {"family": "DS", "OPER_NAME": "D/S1", "라인": "DS-L1", "OPER_NUM": "2400"},
    {"family": "FCB", "OPER_NAME": "FCB1", "라인": "FCB-L1", "OPER_NUM": "2500"},
    {"family": "FCB", "OPER_NAME": "FCB2", "라인": "FCB-L1", "OPER_NUM": "2510"},
    {"family": "FCB", "OPER_NAME": "FCB/H", "라인": "FCB-L2", "OPER_NUM": "2520"},
    {"family": "BM", "OPER_NAME": "B/M", "라인": "BM-L1", "OPER_NUM": "2600"},
    {"family": "PC", "OPER_NAME": "P/C1", "라인": "PC-L1", "OPER_NUM": "2700"},
    {"family": "PC", "OPER_NAME": "P/C2", "라인": "PC-L1", "OPER_NUM": "2710"},
    {"family": "PC", "OPER_NAME": "P/C3", "라인": "PC-L2", "OPER_NUM": "2720"},
    {"family": "PC", "OPER_NAME": "P/C4", "라인": "PC-L2", "OPER_NUM": "2730"},
    {"family": "PC", "OPER_NAME": "P/C5", "라인": "PC-L3", "OPER_NUM": "2740"},
    {"family": "WB", "OPER_NAME": "W/B1", "라인": "WB-L1", "OPER_NUM": "3000"},
    {"family": "WB", "OPER_NAME": "W/B2", "라인": "WB-L1", "OPER_NUM": "3010"},
    {"family": "WB", "OPER_NAME": "W/B3", "라인": "WB-L2", "OPER_NUM": "3020"},
    {"family": "WB", "OPER_NAME": "W/B4", "라인": "WB-L2", "OPER_NUM": "3030"},
    {"family": "WB", "OPER_NAME": "W/B5", "라인": "WB-L3", "OPER_NUM": "3040"},
    {"family": "WB", "OPER_NAME": "W/B6", "라인": "WB-L3", "OPER_NUM": "3050"},
    {"family": "QCSPC", "OPER_NAME": "QCSPC1", "라인": "QC-L1", "OPER_NUM": "3100"},
    {"family": "QCSPC", "OPER_NAME": "QCSPC2", "라인": "QC-L1", "OPER_NUM": "3110"},
    {"family": "QCSPC", "OPER_NAME": "QCSPC3", "라인": "QC-L2", "OPER_NUM": "3120"},
    {"family": "QCSPC", "OPER_NAME": "QCSPC4", "라인": "QC-L2", "OPER_NUM": "3130"},
    {"family": "SAT", "OPER_NAME": "SAT1", "라인": "SAT-L1", "OPER_NUM": "3200"},
    {"family": "SAT", "OPER_NAME": "SAT2", "라인": "SAT-L1", "OPER_NUM": "3210"},
    {"family": "PL", "OPER_NAME": "PLH", "라인": "PL-L1", "OPER_NUM": "3300"},
    {"family": "ETC", "OPER_NAME": "DVI", "라인": "ETC-L1", "OPER_NUM": "3400"},
    {"family": "ETC", "OPER_NAME": "BBMS", "라인": "ETC-L1", "OPER_NUM": "3410"},
    {"family": "ETC", "OPER_NAME": "AVI", "라인": "ETC-L2", "OPER_NUM": "3420"},
    {"family": "ETC", "OPER_NAME": "MDVI", "라인": "ETC-L2", "OPER_NUM": "3430"},
    {"family": "ETC", "OPER_NAME": "MDTI", "라인": "ETC-L3", "OPER_NUM": "3440"},
    {"family": "ETC", "OPER_NAME": "QCSAT", "라인": "ETC-L3", "OPER_NUM": "3450"},
    {"family": "ETC", "OPER_NAME": "LMDI", "라인": "ETC-L4", "OPER_NUM": "3460"},
    {"family": "ETC", "OPER_NAME": "DIC", "라인": "ETC-L4", "OPER_NUM": "3470"},
    {"family": "ETC", "OPER_NAME": "EVI", "라인": "ETC-L5", "OPER_NUM": "3480"},
    {"family": "ETC", "OPER_NAME": "INPUT", "라인": "ETC-L5", "OPER_NUM": "3490"},
]


PROCESS_GROUP_SYNONYMS = {
    group_id: list(group["synonyms"])
    for group_id, group in PROCESS_GROUPS.items()
}


PROCESS_OPER_NUM_MAP = {
    spec["OPER_NAME"]: spec["OPER_NUM"]
    for spec in PROCESS_SPECS
}


PRODUCTS = [
    {
        "MODE": "DDR4",
        "DEN": "256G",
        "TECH": "LC",
        "LEAD": "320",
        "MCP_NO": "A-410A",
        "PKG_TYPE1": "LFBGA",
        "PKG_TYPE2": "SDP",
        "TSV_DIE_TYP": "STD",
    },
    {
        "MODE": "DDR4",
        "DEN": "512G",
        "TECH": "LC",
        "LEAD": "360",
        "MCP_NO": "A-421I",
        "PKG_TYPE1": "FCBGA",
        "PKG_TYPE2": "ODP",
        "TSV_DIE_TYP": "STD",
    },
    {
        "MODE": "DDR5",
        "DEN": "512G",
        "TECH": "FC",
        "LEAD": "420",
        "MCP_NO": "A-587N",
        "PKG_TYPE1": "FCBGA",
        "PKG_TYPE2": "16DP",
        "TSV_DIE_TYP": "STD",
    },
    {
        "MODE": "DDR5",
        "DEN": "1T",
        "TECH": "FC",
        "LEAD": "480",
        "MCP_NO": "A-612B",
        "PKG_TYPE1": "FCBGA",
        "PKG_TYPE2": "ODP",
        "TSV_DIE_TYP": "TSV",
    },
    {
        "MODE": "LPDDR5",
        "DEN": "512G",
        "TECH": "FO",
        "LEAD": "560",
        "MCP_NO": "A-7301",
        "PKG_TYPE1": "LFBGA",
        "PKG_TYPE2": "SDP",
        "TSV_DIE_TYP": "STD",
    },
    {
        "MODE": "LPDDR5",
        "DEN": "1T",
        "TECH": "FO",
        "LEAD": "640",
        "MCP_NO": "A-811V",
        "PKG_TYPE1": "FCBGA",
        "PKG_TYPE2": "16DP",
        "TSV_DIE_TYP": "TSV",
    },
]


ALL_PROCESS_FAMILIES: Set[str] = {
    spec["family"] for spec in PROCESS_SPECS
}


PRODUCT_TECH_FAMILY: Dict[str, Set[str]] = {
    "LC": set(ALL_PROCESS_FAMILIES),
    "FO": set(ALL_PROCESS_FAMILIES),
    "FC": set(ALL_PROCESS_FAMILIES),
}


TECH_GROUPS = {
    "LC": {
        "synonyms": ["LC", "엘씨", "LC제품", "엘시"],
        "actual_values": ["LC"],
        "description": "LC 기술 유형",
    },
    "FO": {
        "synonyms": ["FO", "팬아웃", "FO제품", "fan-out", "Fan-Out", "에프오"],
        "actual_values": ["FO"],
        "description": "Fan-Out 기술 유형",
    },
    "FC": {
        "synonyms": ["FC", "플립칩", "FC제품", "에프씨"],
        "actual_values": ["FC"],
        "description": "Flip Chip 기술 유형",
    },
}


MODE_GROUPS = {
    "DDR4": {
        "synonyms": ["DDR4", "디디알4", "DDR 4"],
        "actual_values": ["DDR4"],
        "description": "DDR4 메모리",
    },
    "DDR5": {
        "synonyms": ["DDR5", "디디알5", "DDR 5"],
        "actual_values": ["DDR5"],
        "description": "DDR5 메모리",
    },
    "LPDDR5": {
        "synonyms": ["LPDDR5", "LP DDR5", "엘피디디알5", "LP5", "저전력DDR5"],
        "actual_values": ["LPDDR5"],
        "description": "저전력 DDR5 메모리",
    },
}


DEN_GROUPS = {
    "256G": {
        "synonyms": ["256G", "256기가", "256Gb", "256gb"],
        "actual_values": ["256G"],
        "description": "256Gb 용량",
    },
    "512G": {
        "synonyms": ["512G", "512기가", "512Gb", "512gb"],
        "actual_values": ["512G"],
        "description": "512Gb 용량",
    },
    "1T": {
        "synonyms": ["1T", "1테라", "1Tb", "1tb", "1TB"],
        "actual_values": ["1T"],
        "description": "1Tb 용량",
    },
}


PKG_TYPE1_GROUPS = {
    "FCBGA": {
        "synonyms": ["FCBGA", "fcbga"],
        "actual_values": ["FCBGA"],
        "description": "FCBGA 패키지 타입",
    },
    "LFBGA": {
        "synonyms": ["LFBGA", "lfbga"],
        "actual_values": ["LFBGA"],
        "description": "LFBGA 패키지 타입",
    },
}


PKG_TYPE2_GROUPS = {
    "ODP": {
        "synonyms": ["ODP", "odp"],
        "actual_values": ["ODP"],
        "description": "ODP 스택 타입",
    },
    "16DP": {
        "synonyms": ["16DP", "16dp"],
        "actual_values": ["16DP"],
        "description": "16DP 스택 타입",
    },
    "SDP": {
        "synonyms": ["SDP", "sdp"],
        "actual_values": ["SDP"],
        "description": "SDP 스택 타입",
    },
}


# 사용자 표현이 여러 개여도 결국 같은 제품 조건으로 모으기 위한 별칭 묶음입니다.
SPECIAL_PRODUCT_ALIASES = {
    "HBM_OR_3DS": ["hbm제품", "hbm자재", "hbm", "3ds", "3ds제품"],
    "AUTO_PRODUCT": ["auto향", "오토향", "차량향", "automotive"],
}


AUTO_SUFFIXES = {"I", "O", "N", "P", "1", "V"}


SPECIAL_DOMAIN_RULES = [
    "투입량, INPUT, 인풋은 INPUT 공정의 생산량(실적)을 의미한다.",
    "HBM제품, HBM자재, 3DS, 3DS제품은 TSV_DIE_TYP 값이 TSV인 제품을 의미한다.",
    "Auto향은 MCP_NO의 마지막 문자가 I, O, N, P, 1, V 중 하나인 제품을 의미한다.",
    "D/S 또는 DS는 기본적으로 D/S1 공정을 의미한다. 사용자가 PKG_TYPE1을 함께 말하면 그 조건도 동시에 적용한다.",
    "DVI는 standalone 공정으로 우선 해석하고, D/I 그룹은 D/I 또는 DI로만 해석한다.",
    "나머지 공정들인 WVI, DVI, BBMS, AVI, MDVI, MDTI, QCSAT, LMDI, DIC, EVI는 공정명 그대로 사용한다.",
]


DATASET_METADATA = {
    "production": {"label": "생산", "keywords": ["생산", "production", "실적", "투입", "input", "인풋"]},
    "target": {"label": "목표", "keywords": ["목표", "target", "계획"]},
    "defect": {"label": "불량", "keywords": ["불량", "defect", "결함"]},
    "equipment": {"label": "설비", "keywords": ["설비", "가동률", "equipment", "downtime"]},
    "wip": {"label": "WIP", "keywords": ["wip", "재공", "대기"]},
    "yield": {"label": "수율", "keywords": ["수율", "yield", "pass rate", "합격률"]},
    "hold": {"label": "홀드", "keywords": ["hold", "홀드", "보류 lot", "hold lot"]},
    "scrap": {"label": "스크랩", "keywords": ["scrap", "스크랩", "폐기", "loss cost", "손실비용"]},
    "recipe": {"label": "레시피", "keywords": ["recipe", "레시피", "공정 조건", "조건값", "parameter", "파라미터"]},
    "lot_trace": {"label": "LOT 이력", "keywords": ["lot", "lot trace", "lot 이력", "추적", "traceability", "로트"]},
}


def build_domain_knowledge_prompt() -> str:
    lines: List[str] = []
    lines.append("=" * 50)
    lines.append("[도메인 지식: 필터 추출 규칙]")
    lines.append("=" * 50)

    lines.append("\n## 사용 가능한 필터 필드")
    for key, field in FILTER_FIELDS.items():
        lines.append(f"- {field['field_name']} ({key}): {field['description']}")

    lines.append("\n## 공정 (process) 필터 규칙")
    lines.append("사용자가 그룹명이나 유사어로 언급하면 actual_values 전체를 process 필터로 사용한다.")
    lines.append("사용자가 개별 공정명으로 언급하면 해당 공정만 process 필터로 사용한다.")
    lines.append("")
    for group in PROCESS_GROUPS.values():
        lines.append(f"### {group['group_name']}")
        lines.append(f"  유사어: {', '.join(group['synonyms'])}")
        lines.append(f"  실제 값: [{', '.join(group['actual_values'])}]")
        lines.append(f"  설명: {group['description']}")
        lines.append("")

    lines.append("### 개별 공정 목록")
    lines.append(f"  {', '.join(INDIVIDUAL_PROCESSES)}")

    lines.append("\n## 공정번호 (OPER_NUM) 규칙")
    lines.append("공정번호는 4자리 숫자이며 공정명과 매핑된다.")
    lines.append("예시:")
    for process_name, oper_num in list(PROCESS_OPER_NUM_MAP.items())[:12]:
        lines.append(f"  - {process_name} -> {oper_num}")
    lines.append("사용자가 공정번호를 말하면 oper_num에 넣고, 공정명이 함께 있으면 두 조건을 동시에 유지한다.")

    lines.append("\n## PKG_TYPE1 규칙")
    for group_id, group in PKG_TYPE1_GROUPS.items():
        lines.append(f"  - {group_id}: 유사어 {', '.join(group['synonyms'])} -> {', '.join(group['actual_values'])}")

    lines.append("\n## PKG_TYPE2 규칙")
    for group_id, group in PKG_TYPE2_GROUPS.items():
        lines.append(f"  - {group_id}: 유사어 {', '.join(group['synonyms'])} -> {', '.join(group['actual_values'])}")

    lines.append("\n## TECH 규칙")
    for group_id, group in TECH_GROUPS.items():
        lines.append(f"  - {group_id}: 유사어 {', '.join(group['synonyms'])} -> {', '.join(group['actual_values'])}")

    lines.append("\n## MODE 규칙")
    for group_id, group in MODE_GROUPS.items():
        lines.append(f"  - {group_id}: 유사어 {', '.join(group['synonyms'])} -> {', '.join(group['actual_values'])}")

    lines.append("\n## DEN 규칙")
    for group_id, group in DEN_GROUPS.items():
        lines.append(f"  - {group_id}: 유사어 {', '.join(group['synonyms'])} -> {', '.join(group['actual_values'])}")

    lines.append("\n## 특수 용어 규칙")
    for rule in SPECIAL_DOMAIN_RULES:
        lines.append(f"- {rule}")

    lines.append("\n## HBM/Auto향 해석 규칙")
    lines.append("- HBM 또는 3DS 계열 표현은 OR 조건으로 같은 의미다. product_name에 'HBM_OR_3DS'를 넣어 downstream matcher가 TSV_DIE_TYP='TSV' 조건으로 해석하게 한다.")
    lines.append("- Auto향을 들으면 product_name에 'AUTO_PRODUCT'를 넣어 downstream matcher가 MCP_NO suffix 규칙으로 해석하게 한다.")
    lines.append("- INPUT/투입량/인풋을 들으면 process를 ['INPUT']으로 추출한다.")

    lines.append("\n## 공통 추출 규칙")
    lines.append("1. 사용자가 언급하지 않은 필드는 null로 설정한다.")
    lines.append("2. 여러 값을 나열하면 리스트로 추출한다.")
    lines.append("3. 그룹명과 개별값을 구분한다.")
    lines.append("4. 공정, OPER_NUM, PKG_TYPE1, PKG_TYPE2가 함께 나오면 가능한 조건을 모두 유지한다.")
    lines.append("5. mcp_no는 MCP_NO 문자열 그대로 유지한다.")

    return "\n".join(lines)


if __name__ == "__main__":
    print(build_domain_knowledge_prompt())
