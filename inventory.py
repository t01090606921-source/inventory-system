import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# 1. 시스템 설정 (가장 먼저 실행)
st.set_page_config(page_title="AS TAT 시스템", layout="wide")

# Supabase 연결 시도 및 에러 체크
try:
    url: str = st.secrets["SUPABASE_URL"]
    key: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error(f"❌ 접속 설정 오류: {e}")
    st.stop()

st.title("🚀 AS TAT 분석 시스템 (최종 점검 버전)")

# --- [전수 로드 함수] ---
def fetch_all_data(table_name, columns="*"):
    all_data = []
    limit = 1000
    offset = 0
    while True:
        try:
            res = supabase.table(table_name).select(columns).range(offset, offset + limit - 1).execute()
            all_data.extend(res.data)
            if len(res.data) < limit: break
            offset += limit
        except: break
    return pd.DataFrame(all_data)

# --- 2. 사이드바 (마스터 등록 및 초기화) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    # 마스터 업로드 버튼 강제 노출
    st.subheader("1. 마스터 데이터 등록")
    master_file = st.file_uploader("마스터 엑셀 선택", type=['xlsx'], key="m_key")
    
    if master_file and st.button("🚀 마스터 강제 재등록"):
        with st.spinner("데이터 동기화 중..."):
            m_df = pd.read_excel(master_file, dtype=str)
            t_col = next((c for c in m_df.columns if "품목코드" in str(c) or "자재번호" in str(c)), m_df.columns[0])
            m_data = [{"자재번호": str(row[t_col]).strip().upper(), 
                       "공급업체명": str(row.iloc[5]).strip() if len(row)>5 else "정보누락",
                       "분류구분": str(row.iloc[10]).strip() if len(row)>10 else "정보누락"} 
                      for _, row in m_df.iterrows() if not pd.isna(row[t_col])]
            if m_data:
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_data), 200):
                    supabase.table("master_data").insert(m_data[i:i+200]).execute()
                st.success("✅ 등록 완료!")
                st.rerun()

    # 초기화 버튼
    st.divider()
    if st.button("⚠️ 데이터 전체 초기화", type="primary"):
        supabase.table("as_history").delete().neq("id", -1).execute()
        st.rerun()

# --- 3. 메인 기능 (입고 / 출고) ---
st.header("📥 AS 입고 / 📤 AS 출고")
col1, col2 = st.columns(2)

with col1:
    st.subheader("입고 처리")
    in_file = st.file_uploader("입고 엑셀", type=['xlsx'], key="in_key")
    if in_file and st.button("🚀 입고 실행"):
        m_df_local = fetch_all_data("master_data")
        m_lookup = m_df_local.set_index('자재번호').to_dict('index') if not m_df_local.empty else {}
        
        df = pd.read_excel(in_file, dtype=str)
        as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
        
        recs = []
        for i, (_, row) in enumerate(as_in.iterrows()):
            mat_val = str(row.iloc[3]).strip().upper()
            m_info = m_lookup.get(mat_val)
            recs.append({
                "압축코드": str(row.iloc[7]).strip(), "자재번호": mat_val,
                "규격": str(row.iloc[5]).strip(), "상태": "출고 대기",
                "공급업체명": m_info['공급업체명'] if m_info else "미등록",
                "분류구분": m_info['분류구분'] if m_info else "미등록",
                "입고일": pd.to_datetime(row.iloc[1]).strftime('%Y-%m-%d')
            })
            if len(recs) == 1000:
                supabase.table("as_history").insert(recs).execute()
                recs = []
        if recs: supabase.table("as_history").insert(recs).execute()
        st.success("✅ 입고 완료")

with col2:
    st.subheader("출고 처리")
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out_key")
    if out_file and st.button("🚀 출고 실행"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = [str(r).strip() for r in as_out.iloc[:, 10]]
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            for i in range(0, len(out_keys), 500):
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"}).in_("압축코드", out_keys[i:i+500]).eq("상태", "출고 대기").execute()
            st.success("✅ 출고 완료")

# --- 4. TAT 통계 분석 (경량 리포트) ---
st.divider()
st.header("📊 수리대상 TAT 분석 리포트")

if st.button("📈 통계 분석 실행 (데이터 전수 로드)", use_container_width=True):
    with st.spinner("57만 건 데이터 분석 중..."):
        df_raw = fetch_all_data("as_history", "입고일, 출고일, 공급업체명, 분류구분")
    
    if not df_raw.empty:
        df_raw['입고일'] = pd.to_datetime(df_raw['입고일'], errors='coerce')
        df_raw['출고일'] = pd.to_datetime(df_raw['출고일'], errors='coerce')
        
        # 수리대상 필터링 및 TAT 계산
        df_rep = df_raw[df_raw['분류구분'] == '수리대상'].copy()
        df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
        
        m1, m2 = st.columns(2)
        m1.metric("전체 수리대상 건수", f"{len(df_rep):,} 건")
        m2.metric("평균 TAT (완료건 기준)", f"{df_rep['TAT'].mean():.1f} 일")

        # 업체별 요약
        summary = df_rep[df_rep['출고일'].notna()].groupby('공급업체명').agg(
            완료건수=('TAT', 'count'), 평균TAT=('TAT', 'mean')
        ).reset_index()
        summary['평균TAT'] = summary['평균TAT'].round(1)
        st.table(summary.sort_values('평균TAT'))
        
        # 상세 결과 다운로드
        csv = df_rep.to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 수리대상 데이터 다운로드", csv, "TAT_Report.csv", "text/csv")
    else:
        st.info("조회할 데이터가 없습니다.")
