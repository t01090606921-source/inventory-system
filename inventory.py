import streamlit as st
import pandas as pd
from supabase import create_client, Client
import io

# --- 1. Supabase 접속 설정 ---
url: str = st.secrets["SUPABASE_URL"]
key: str = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.set_page_config(page_title="AS TAT 분석 시스템", layout="wide")
st.title("🚀 AS TAT 분석 시스템 (57만 건 대응)")

# --- 2. [함수] 데이터 전수 로드 (Pagination) ---
def fetch_all_data(table_name, columns="*"):
    all_data = []
    limit = 1000
    offset = 0
    while True:
        res = supabase.table(table_name).select(columns).range(offset, offset + limit - 1).execute()
        all_data.extend(res.data)
        if len(res.data) < limit: break
        offset += limit
    return pd.DataFrame(all_data)

# --- 3. 사이드바: 마스터 등록 및 관리 (UI 고정) ---
with st.sidebar:
    st.header("⚙️ 시스템 관리")
    
    # [복구] 마스터 파일 업로더를 최상단에 배치
    st.subheader("1. 마스터 데이터 관리")
    master_file = st.file_uploader("마스터 엑셀 업로드", type=['xlsx'], key="master_upload_fixed")
    
    if master_file and st.button("🚀 마스터 강제 재등록", use_container_width=True):
        m_df_raw = pd.read_excel(master_file, dtype=str)
        t_col = next((c for c in m_df_raw.columns if "품목코드" in str(c) or "자재번호" in str(c)), m_df_raw.columns[0])
        
        m_data = []
        for _, row in m_df_raw.iterrows():
            mat_val = str(row[t_col]).strip().upper()
            if not mat_val or mat_val == "NAN": continue
            m_data.append({
                "자재번호": mat_val,
                "공급업체명": str(row.iloc[5]).strip() if len(row)>5 else "정보누락",
                "분류구분": str(row.iloc[10]).strip() if len(row)>10 else "정보누락"
            })
        
        if m_data:
            with st.spinner("마스터 갱신 중..."):
                supabase.table("master_data").delete().neq("자재번호", "EMPTY").execute()
                for i in range(0, len(m_data), 200):
                    supabase.table("master_data").insert(m_data[i:i+200]).execute()
            st.success("✅ 마스터 등록 완료")
            st.rerun()

    # 마스터 상태 표시
    try:
        m_df_local = fetch_all_data("master_data")
        st.metric("동기화된 마스터 건수", f"{len(m_df_local):,} 건")
    except:
        m_df_local = pd.DataFrame()

    st.divider()
    if st.button("⚠️ 데이터 전체 초기화", type="primary", use_container_width=True):
        supabase.table("as_history").delete().neq("id", -1).execute()
        st.rerun()

# --- 4. 입고 / 출고 관리 탭 ---
tab1, tab2 = st.tabs(["📥 대량 입고 (수신)", "📤 대량 출고 (송신)"])

with tab1:
    in_file = st.file_uploader("입고 엑셀 (최대 60만 건)", type=['xlsx'], key="in_file")
    if in_file and st.button("🚀 입고 및 매칭 시작"):
        if m_df_local.empty:
            st.error("사이드바에서 마스터를 먼저 등록해주세요.")
        else:
            m_lookup = m_df_local.set_index('자재번호').to_dict('index')
            df = pd.read_excel(in_file, dtype=str)
            as_in = df[df.iloc[:, 0].str.contains('A/S 철거', na=False)].copy()
            
            total_rows, recs = len(as_in), []
            prog_bar, status_text = st.progress(0), st.empty()
            
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
                    prog_bar.progress((i + 1) / total_rows)
                    status_text.text(f"처리 중: {i+1:,} / {total_rows:,} 건")
            if recs: supabase.table("as_history").insert(recs).execute()
            st.success("✅ 입고 완료!")

with tab2:
    out_file = st.file_uploader("출고 엑셀", type=['xlsx'], key="out_file")
    if out_file and st.button("🚀 출고 대량 처리"):
        df_out = pd.read_excel(out_file, dtype=str)
        as_out = df_out[df_out.iloc[:, 3].str.contains('AS 카톤 박스', na=False)].copy()
        if not as_out.empty:
            out_keys = [str(r).strip() for r in as_out.iloc[:, 10]]
            out_date = pd.to_datetime(as_out.iloc[0, 6]).strftime('%Y-%m-%d')
            for i in range(0, len(out_keys), 500):
                batch = out_keys[i:i+500]
                supabase.table("as_history").update({"출고일": out_date, "상태": "출고 완료"})\
                        .in_("압축코드", batch).eq("상태", "출고 대기").execute()
            st.success("✅ 출고 완료")

# --- 5. [경량화 핵심] 수리대상 TAT 통계 리포트 ---
st.divider()
st.subheader("📊 수리대상 TAT 분석 리포트")
if st.button("📈 통계 분석 실행 (데이터 전수 로드)", use_container_width=True):
    with st.spinner("57만 건 데이터 분석 중..."):
        # 필요한 컬럼만 최소한으로 로드 (속도 최적화)
        df_raw = fetch_all_data("as_history", "입고일, 출고일, 공급업체명, 분류구분")
    
    if not df_raw.empty:
        # 데이터 처리
        df_raw['입고일'] = pd.to_datetime(df_raw['입고일'], errors='coerce')
        df_raw['출고일'] = pd.to_datetime(df_raw['출고일'], errors='coerce')
        
        # '수리대상' 필터링 및 TAT 계산
        df_rep = df_raw[df_raw['분류구분'] == '수리대상'].copy()
        df_rep['TAT'] = (df_rep['출고일'] - df_rep['입고일']).dt.days
        
        # 메트릭
        m1, m2, m3 = st.columns(3)
        m1.metric("전체 수리대상", f"{len(df_rep):,} 건")
        m2.metric("평균 TAT", f"{df_rep['TAT'].mean():.1f} 일")
        m3.metric("수리 완료율", f"{(df_rep['출고일'].notna().sum() / len(df_rep) * 100):.1f}%")

        # 업체별 요약 통계
        st.write("### 🏢 업체별 평균 TAT (소요기간)")
        summary = df_rep[df_rep['출고일'].notna()].groupby('공급업체명').agg(
            수리완료건수=('TAT', 'count'),
            평균TAT=('TAT', 'mean')
        ).reset_index()
        summary['평균TAT'] = summary['평균TAT'].round(1)
        st.dataframe(summary.sort_values('평균TAT'), use_container_width=True, hide_index=True)
        
        # 상세 데이터 다운로드 (CSV가 대용량에 더 안전함)
        csv = df_rep.to_csv(index=False).encode('utf-8-sig')
        st.download_button("📥 수리대상 상세결과 다운로드(CSV)", csv, "Repair_TAT_Report.csv", "text/csv")
    else:
        st.info("조회할 히스토리 데이터가 없습니다.")
