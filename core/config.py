import os

from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI


load_dotenv()


def get_llm(temperature: float = 0.0):
    api_key = os.getenv("LLM_API_KEY", "").strip()
    if not api_key:
        raise ValueError("LLM_API_KEY 환경 변수가 설정되어 있지 않습니다.")

    return ChatGoogleGenerativeAI(
        model="gemini-flash-latest",
        google_api_key=api_key,
        temperature=temperature,
    )


SYSTEM_PROMPT = """너는 제조 생산 데이터 조회와 후속 분석을 돕는 AI 에이전트다.

원칙:
- 먼저 사용자의 질문이 원본 데이터 조회인지, 현재 데이터 후속 분석인지 구분한다.
- 새 조회가 필요하면 필수 파라미터만 추출해 원본 데이터를 가져온다.
- 현재 데이터가 있으면 후속 질문은 가능한 한 pandas 기반 변환으로 처리한다.
- 존재하지 않는 데이터나 컬럼은 지어내지 않는다.
- 답변은 항상 현재 결과 테이블 기준으로 설명한다.
"""
