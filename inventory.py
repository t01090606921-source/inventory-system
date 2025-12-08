import streamlit as st
import pandas as pd
from datetime import datetime
import io

# 구글 시트 라이브러리 (로컬 실행 시 설치 필요: pip install gspread oauth2client)
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
except ImportError:
    st.error("라이브러리가 설치되지 않았습니다. pip install gspread oauth2client 명령어를 실행하세요.")
    st.stop()

# --- [1] 로그인 보안 설정 ---
def check_password():
    if 'password_correct' not in st.session_state:
        st.session_state.password_correct = False
    if st.session_state.password_correct:
        return True
    
    st.set_page_config(page_title="로그인", layout="centered")
    st.title("🔒 관계자 외 출입금지")
    pwd = st.text_input("비밀번호를 입력하세요", type="password")
    if st.button("로그인"):
        if pwd == "1234": # 비밀번호 변경 가능
            st.session_state.password_correct = True
            st.rerun()
        else:
            st.error("비밀번호가 틀렸습니다.")
    return False

if not check_password():
    st.stop()

# --- [2] 구글 시트 연결 설정 ---
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
SHEET_NAME = '재고관리_데이터'

def get_google_sheet_client():
    try:
        # Streamlit Cloud 배포 환경
        if "gcp_service_account" in st.secrets:
            creds_dict = dict(st.session_state.secrets["gcp_service_account"])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
        # 로컬 테스트 환경
        else:
            return None
            
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        return None

# --- 데이터 읽기/쓰기 함수 ---
def load_data():
    client = get_google_sheet_client()
    
    # 1. 구글 시트 모드
    if client:
        try:
            sh = client.open(SHEET_NAME)
            
            def get_ws_df(name, cols):
                try:
                    ws = sh.worksheet(name)
                    records = ws.get_all_records()
                    df = pd.DataFrame(records)
                    # [핵심 수정] 데이터가 비어있거나 필수 컬럼이 없으면 강제 생성
                    if df.empty or not set(cols).issubset(df.columns):
                        df = pd.DataFrame(columns=cols)
                except:
                    # 시트가 없으면 생성하고 헤더 추가
                    ws = sh.add_worksheet(title=name, rows=1000, cols=20)
                    ws.append_row(cols)
                    df = pd.DataFrame(columns=cols)
                
                # 강제로 컬럼 순서 및 존재 여부 보장
                for c in cols:
                    if c not in df.columns:
                        df[c] = ""
                return df[cols] # 컬럼 순서 정렬

            df_m = get_ws_df('품목표', ['품목코드', '품명', '규격', '분류구분', '공급업체', '바코드'])
            df_map = get_ws_df('매핑정보', ['Box번호', '품목코드', '수량'])
            df_l = get_ws_df('입출고', ['날짜', '구분', 'Box번호', '위치', '파렛트'])
            df_d = get_ws_df('상세내역', ['Box번호', '품목코드', '규격', '압축코드'])
            
            # 전처리
            if not df_map.empty and '수량' in df_map.columns:
                df_map['수량'] = pd.to_numeric(df_map['수량'], errors='coerce').fillna(0).astype(int)
                df_map = df_map.drop_duplicates(subset=['Box번호'], keep='last')
            
            return df_m, df_map, df_l, df_d, True

        except Exception as e:
            st.error(f"구글 시트 로드 중 오류: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), False

    # 2. 로컬 모드 (엑셀 파일 사용)
    else:
        import os
        FILE_NAME = 'inventory_data.xlsx'
        if not os.path.exists(FILE_NAME):
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), False
        try:
            df_m = pd.read_excel(FILE_NAME, sheet_name='품목표', dtype=str)
            df_map = pd.read_excel(FILE_NAME, sheet_name='매핑정보', dtype={'Box번호': str, '품목코드': str, '수량': int})
            df_l = pd.read_excel(FILE_NAME, sheet_name='입출고', dtype={'Box번호': str})
            try: df_d = pd.read_excel(FILE_NAME, sheet_name='상세내역', dtype=str)
            except: df_d = pd.DataFrame(columns=['Box번호', '품목코드', '규격', '압축코드'])
            
            if not df_map.empty: df_map = df_map.drop_duplicates(subset=['Box번호'], keep='last')
            
            return df_m, df_map, df_l, df_d, False
        except:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), False

def save_log_data(new_df):
    client = get_google_sheet_client()
    if client:
        try:
            sh = client.open(SHEET_NAME)
            ws = sh.worksheet('입출고')
            ws.append_rows(new_df.values.tolist())
            return True
        except:
            return False
    else:
        FILE_NAME = 'inventory_data.xlsx'
        if os.path.exists(FILE_NAME):
            with pd.ExcelWriter(FILE_NAME, mode='a', if_sheet_exists='overlay') as writer:
                 pass 
        return True

def save_data(sheet_name, new_df):
    client = get_google_sheet_client()
    if client:
        try:
            sh = client.open(SHEET_NAME)
            try:
                ws = sh.worksheet(sheet_name)
                ws.clear() # 기존 데이터 삭제 후 덮어쓰기 (마스터 데이터 등)
                ws.update([new_df.columns.values.tolist()] + new_df.values.tolist())
            except:
                ws = sh.add_worksheet(title=sheet_name, rows=1000, cols=20)
                ws.update([new_df.columns.values.tolist()] + new_df.values.tolist())
            return True
        except Exception as e:
            st.error(f"저장 실패: {e}")
            return False
    return False

# --- 초기화 ---
def init_data():
    if 'df_master' not in st.session_state:
        m, map, l, d, is_cloud = load_data()
        st.session_state.df_master = m
        st.session_state.df_mapping = map
        st.session_state.df_log = l
        st.session_state.df_details = d
        st.session_state.is_cloud = is_cloud

# --- 엑셀 다운로드 ---
def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

# --- 랙 맵 렌더링 ---
def render_rack_map_interactive(stock_df, highlight_locs=None):
    if highlight_locs is None: highlight_locs = []
    rack_summary = {}
    for _, row in stock_df.iterrows():
        raw_loc = str(row.get('위치', '')).strip()
        if not raw_loc or raw_loc == '미지정': continue
        parts = raw_loc.split('-')
        if len(parts) >= 3: k = f"{parts[0]}-{parts[2]}"
        elif len(parts) == 2: k = f"{parts[0]}-{parts[1]}"
        else: k = raw_loc
        rack_summary[k] = rack_summary.get(k, 0) + 1

    st.markdown("""
    <style>
    div[data-testid="column"] { padding: 0 2px !important; min-width: 0 !important; }
    div.stButton > button { width: 100%; height: 40px !important; margin: 2px 0px !important; padding: 0px !important; font-size: 10px !important; font-weight: 700 !important; border-radius: 4px !important; border: 1px solid #ccc; box-shadow: 1px 1px 2px rgba(0,0,0,0.05); }
    div.stButton > button:hover { border-color: #333 !important; transform: scale(1.05); z-index: 5; }
    button[kind="primary"] { background-color: #ffcdd2 !important; color: #b71c1c !important; border: 2px solid #d32f2f !important; }
    button[kind="secondary"] { background-color: #ffffff !important; color: #555 !important; }
    .rack-divider { border-left: 2px dashed #ddd; height: 100%; margin: 0 auto; }
    .rack-spacer { height: 25px; width: 100%; }
    .rack7-label { text-align: center; font-weight: bold; color: #555; margin-bottom: 5px; font-size: 12px; }
    </style>
    """, unsafe_allow_html=True)

    def rack_click(key):
        st.session_state.selected_rack = key
        st.session_state.filter_mode = 'rack'

    c_left, c_mid, c_right = st.columns([3.5, 0.1, 0.8])
    with c_left:
        for r_num in [6]:
            cols = st.columns(7)
            for c_idx, col in enumerate(cols):
                rack_key = f"{r_num}-{c_idx+1}"
                qty = rack_summary.get(rack_key, 0)
                label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
                is_hl = (rack_key in highlight_locs) or (rack_key == st.session_state.selected_rack)
                btn_type = "primary" if is_hl else "secondary"
                col.button(label, key=f"btn_{rack_key}", type=btn_type, on_click=rack_click, args=(rack_key,), use_container_width=True)
        st.markdown('<div class="rack-spacer"></div>', unsafe_allow_html=True)
        for r_num in [5, 4]:
            cols = st.columns(7)
            for c_idx, col in enumerate(cols):
                rack_key = f"{r_num}-{c_idx+1}"
                qty = rack_summary.get(rack_key, 0)
                label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
                is_hl = (rack_key in highlight_locs) or (rack_key == st.session_state.selected_rack)
                btn_type = "primary" if is_hl else "secondary"
                col.button(label, key=f"btn_{rack_key}", type=btn_type, on_click=rack_click, args=(rack_key,), use_container_width=True)
        st.markdown('<div class="rack-spacer"></div>', unsafe_allow_html=True)
        for r_num in [3, 2, 1]:
            cols = st.columns(7)
            for c_idx, col in enumerate(cols):
                rack_key = f"{r_num}-{c_idx+1}"
                qty = rack_summary.get(rack_key, 0)
                label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
                is_hl = (rack_key in highlight_locs) or (rack_key == st.session_state.selected_rack)
                btn_type = "primary" if is_hl else "secondary"
                col.button(label, key=f"btn_{rack_key}", type=btn_type, on_click=rack_click, args=(rack_key,), use_container_width=True)
    with c_mid:
        st.markdown('<div class="rack-divider"></div>', unsafe_allow_html=True)
    with c_right:
        st.markdown('<div class="rack7-label">Rack 7</div>', unsafe_allow_html=True)
        for i in range(12, 0, -1):
            rack_key = f"7-{i}"
            qty = rack_summary.get(rack_key, 0)
            label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
            is_hl = (rack_key in highlight_locs) or (rack_key == st.session_state.selected_rack)
            btn_type = "primary" if is_hl else "secondary"
            st.button(label, key=f"btn_{rack_key}", type=btn_type, on_click=rack_click, args=(rack_key,), use_container_width=True)

def refresh_all():
    st.cache_data.clear()
    if 'data_loaded' in st.session_state: del st.session_state.data_loaded
    st.rerun()

# --- 메인 실행 ---
def main():
    st.title("🏭 디지타스 창고 재고관리 (Ver.5.2)")
    
    if 'proc_msg' not in st.session_state: st.session_state.proc_msg = None
    if 'scan_buffer' not in st.session_state: st.session_state.scan_buffer = []
    if 'selected_rack' not in st.session_state: st.session_state.selected_rack = None
    if 'filter_mode' not in st.session_state: st.session_state.filter_mode = 'all'

    init_data()

    df_master = st.session_state.df_master
    df_mapping = st.session_state.df_mapping
    df_log = st.session_state.df_log
    df_details = st.session_state.df_details
    today_str = datetime.now().strftime("%Y%m%d")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["1. 연속 스캔", "2. 재고 현황", "3. 일괄 업로드", "4. 포장데이터", "5. 품목 마스터"])

    with tab1:
        c_h, c_r = st.columns([4, 1])
        with c_h: st.subheader("🚀 스캔 작업")
        with c_r: 
            if st.button("🔄 새로고침", use_container_width=True, key='r1'): refresh_all()

        if st.session_state.proc_msg:
            m_type, m_text = st.session_state.proc_msg
            if m_type == 'success': st.success(m_text)
            elif m_type == 'error': st.error(m_text)
            else: st.info(m_text)

        c1, c2, c3, c4 = st.columns([1.5, 1, 1, 2])
        with c1: st.radio("모드", ["입고", "재고이동", "출고", "조회(검색)"], horizontal=True, key="work_mode")
        with c2: st.text_input("적재 위치 (1-2-7)", key="curr_location")
        with c3: st.text_input("파렛트 이름", key="curr_palette")
        with c4: st.text_input("Box 번호 스캔", key="scan_input", on_change=buffer_scan)

        st.dataframe(pd.DataFrame(st.session_state.scan_buffer).iloc[::-1], use_container_width=True, height=150)
        
        save_label = "💾 구글 시트에 저장" if st.session_state.is_cloud else "💾 로컬 저장"
        if st.button(save_label, type="primary", use_container_width=True): 
            if not st.session_state.scan_buffer:
                st.warning("저장할 데이터가 없습니다.")
            else:
                new_logs = pd.DataFrame(st.session_state.scan_buffer)
                if st.session_state.is_cloud:
                    with st.spinner('구글 시트에 저장 중...'):
                        if save_log_data(new_logs):
                            st.session_state.df_log = pd.concat([st.session_state.df_log, new_logs], ignore_index=True)
                            st.session_state.scan_buffer = []
                            st.session_state.proc_msg = ("success", "✅ 구글 시트에 저장되었습니다!")
                            st.rerun()
                        else: st.error("저장 실패")
                else:
                    st.session_state.df_log = pd.concat([st.session_state.df_log, new_logs], ignore_index=True)
                    st.session_state.scan_buffer = []
                    st.session_state.proc_msg = ("success", "✅ 저장되었습니다!")
                    st.rerun()

        if st.button("🗑️ 목록 비우기", use_container_width=True): st.session_state.scan_buffer = []

    with tab2:
        # [핵심 수정] 데이터가 없으면 계산 건너뛰기
        if df_log.empty:
            st.info("데이터가 없습니다. [3. 일괄 업로드] 탭에서 엑셀을 업로드하거나 [1. 연속 스캔]으로 데이터를 입력하세요.")
        else:
            try:
                # 재고 계산
                last_stat = df_log.sort_values('날짜').groupby('Box번호').tail(1)
                stock_boxes = last_stat[last_stat['구분'].isin(['입고', '이동'])]
                merged = pd.merge(stock_boxes, df_mapping, on='Box번호', how='left')
                merged['위치'] = merged['위치'].fillna('미지정').replace('', '미지정')
                merged['파렛트'] = merged['파렛트'].fillna('이름없음').replace('', '이름없음')
                merged = pd.merge(merged, df_master, on='품목코드', how='left')

                sc1, sc2, sc3 = st.columns([1, 1, 2])
                with sc1: search_target = st.selectbox("검색 기준", ["전체", "품목코드", "규격", "Box번호"])
                with sc2: exact_match = st.checkbox("정확히 일치")
                with sc3: search_query = st.text_input("검색어", key="sq")

                filtered_df = merged.copy()
                hl_list = []

                if search_query:
                    q = search_query.strip()
                    if exact_match: mask = filtered_df['품목코드'] == q
                    else: mask = filtered_df['품목코드'].astype(str).str.contains(q, na=False)
                    filtered_df = filtered_df[mask]
                    hl_list = [str(x).split('-')[0]+'-'+str(x).split('-')[2] for x in filtered_df['위치'] if len(str(x).split('-'))>=3]
                
                if st.session_state.selected_rack:
                    sel = st.session_state.selected_rack
                    hl_list.append(sel)
                    def check_loc(l):
                        p = str(l).split('-')
                        return (len(p)>=3 and f"{p[0]}-{p[2]}"==sel) or (len(p)==2 and f"{p[0]}-{p[1]}"==sel)
                    filtered_df = filtered_df[filtered_df['위치'].apply(check_loc)]

                render_rack_map_interactive(stock_boxes, hl_list)
                st.dataframe(filtered_df)
            except Exception as e:
                st.error(f"데이터 처리 중 오류 발생: {e}")

    with tab3:
        st.info("입출고 내역을 엑셀로 한 번에 올릴 수 있습니다.")
        up = st.file_uploader("입출고 파일", type=['xlsx', 'csv'])
        if up and st.button("구글 시트 업로드"):
            df = pd.read_excel(up) if up.name.endswith('xlsx') else pd.read_csv(up)
            # 필수 컬럼 체크
            req_cols = ['구분', 'Box번호']
            if not set(req_cols).issubset(df.columns):
                st.error(f"필수 컬럼이 없습니다: {req_cols}")
            else:
                if '날짜' not in df.columns: df['날짜'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if '위치' not in df.columns: df['위치'] = ""
                if '파렛트' not in df.columns: df['파렛트'] = ""
                
                with st.spinner("업로드 중..."):
                    if save_data('입출고', df[['날짜', '구분', 'Box번호', '위치', '파렛트']]):
                        refresh_all()
                        st.success("완료!")

    with tab4:
        st.info("포장 데이터(매핑정보) 업로드")
        up_pack = st.file_uploader("포장 파일", type=['xlsx'])
        if up_pack and st.button("등록"):
            try:
                raw = pd.read_excel(up_pack, dtype=str)
                # 매핑정보 생성
                grp = raw.groupby(['카톤박스번호', '박스자재코드']).size().reset_index(name='수량')
                grp.columns = ['Box번호', '품목코드', '수량']
                
                # 상세내역 생성
                if '압축코드' in raw.columns:
                    dets = raw[['카톤박스번호', '박스자재코드', '박스자재규격', '압축코드']].copy()
                    dets.columns = ['Box번호', '품목코드', '규격', '압축코드']
                else:
                    dets = pd.DataFrame(columns=['Box번호', '품목코드', '규격', '압축코드'])

                # 품목마스터 생성
                items = raw[['박스자재코드', '박스자재명', '박스자재규격', '출고처명']].drop_duplicates('박스자재코드')
                items.columns = ['품목코드', '품명', '규격', '공급업체']
                items['분류구분'] = ''
                items['바코드'] = ''

                with st.spinner("구글 시트에 등록 중..."):
                    save_data('매핑정보', grp)
                    save_data('상세내역', dets)
                    save_data('품목표', items)
                    refresh_all()
                    st.success("포장 데이터 등록 완료!")
            except Exception as e:
                st.error(f"오류: {e}")

    with tab5:
        st.dataframe(df_master)

if __name__ == '__main__':
    main()
