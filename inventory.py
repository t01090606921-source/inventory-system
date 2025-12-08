import streamlit as st
import pandas as pd
from datetime import datetime
import io

# 구글 시트 라이브러리
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
except ImportError:
    st.error("라이브러리 설치 필요. requirements.txt 확인")
    st.stop()

# --- [1] 로그인 보안 ---
def check_password():
    if 'password_correct' not in st.session_state:
        st.session_state.password_correct = False
    if st.session_state.password_correct:
        return True
    
    st.set_page_config(page_title="재고관리", layout="wide")
    st.title("🔒 관계자 외 출입금지")
    pwd = st.text_input("비밀번호를 입력하세요", type="password")
    if st.button("로그인"):
        if pwd == "1234": 
            st.session_state.password_correct = True
            st.rerun()
        else:
            st.error("비밀번호가 틀렸습니다.")
    return False

if not check_password():
    st.stop()

# --- [2] 구글 시트 연결 ---
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
SHEET_NAME = '재고관리_데이터'

def get_google_sheet_client():
    try:
        if "gcp_service_account" in st.secrets:
            creds_dict = dict(st.secrets["gcp_service_account"])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
            client = gspread.authorize(creds)
            return client
        else: return None
    except: return None

# --- 데이터 로드 (오류 방지 강화) ---
def load_data():
    client = get_google_sheet_client()
    if client:
        try:
            sh = client.open(SHEET_NAME)
            
            def get_ws_df(name, cols):
                try:
                    ws = sh.worksheet(name)
                    records = ws.get_all_records()
                    df = pd.DataFrame(records)
                    # 데이터가 없으면 빈 프레임 생성
                    if df.empty: df = pd.DataFrame(columns=cols)
                except:
                    # 시트가 없으면 새로 생성
                    ws = sh.add_worksheet(title=name, rows=1000, cols=20)
                    ws.append_row(cols)
                    df = pd.DataFrame(columns=cols)
                
                # [강화 1] 컬럼명 앞뒤 공백 제거 (실수 방지)
                df.columns = df.columns.astype(str).str.strip()

                # [강화 2] 필수 컬럼 누락 시 강제 생성 (KeyError 방지)
                for c in cols:
                    if c not in df.columns: 
                        df[c] = ""
                
                # 지정된 컬럼만 순서대로 가져오기
                df = df[cols]
                
                # 데이터 내용 문자열 변환 및 정리
                df = df.astype(str).apply(lambda x: x.str.replace(r'\.0$', '', regex=True).str.strip())
                return df

            df_m = get_ws_df('품목표', ['품목코드', '품명', '규격', '분류구분', '공급업체', '바코드'])
            df_map = get_ws_df('매핑정보', ['Box번호', '품목코드', '수량'])
            df_l = get_ws_df('입출고', ['날짜', '구분', 'Box번호', '위치', '파렛트'])
            df_d = get_ws_df('상세내역', ['Box번호', '품목코드', '규격', '압축코드'])
            
            # 수량 컬럼 숫자 변환
            if not df_map.empty:
                df_map['수량'] = pd.to_numeric(df_map['수량'], errors='coerce').fillna(0).astype(int)
                df_map = df_map.drop_duplicates(subset=['Box번호'], keep='last')
            
            return df_m, df_map, df_l, df_d, True
        except Exception as e:
            st.error(f"데이터 로드 중 오류 발생: {e}")
            # 오류 나도 빈 깡통 반환해서 앱이 죽지 않게 함
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), False
    else:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), False

def save_log_data(new_df):
    client = get_google_sheet_client()
    if client:
        try:
            sh = client.open(SHEET_NAME)
            ws = sh.worksheet('입출고')
            save_cols = ['날짜', '구분', 'Box번호', '위치', '파렛트']
            # 저장 전 데이터 정리
            valid_df = new_df[save_cols].astype(str).apply(lambda x: x.str.strip())
            ws.append_rows(valid_df.values.tolist())
            return True
        except: return False
    return True

def save_data(sheet_name, new_df):
    client = get_google_sheet_client()
    if client:
        try:
            sh = client.open(SHEET_NAME)
            try:
                ws = sh.worksheet(sheet_name)
                ws.clear()
                up_df = new_df.astype(str).apply(lambda x: x.str.replace(r'\.0$', '', regex=True).str.strip())
                ws.update([up_df.columns.values.tolist()] + up_df.values.tolist())
            except:
                ws = sh.add_worksheet(title=sheet_name, rows=1000, cols=20)
                up_df = new_df.astype(str).apply(lambda x: x.str.replace(r'\.0$', '', regex=True).str.strip())
                ws.update([up_df.columns.values.tolist()] + up_df.values.tolist())
            return True
        except: return False
    return False

def init_data():
    if 'df_master' not in st.session_state:
        m, map, l, d, is_cloud = load_data()
        st.session_state.df_master = m
        st.session_state.df_mapping = map
        st.session_state.df_log = l
        st.session_state.df_details = d
        st.session_state.is_cloud = is_cloud

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def get_sample_file():
    sample_data = {
        '날짜': [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        '구분': ['입고'],
        'Box번호': ['V2024...'],
        '위치': ['1-2-7'],
        '파렛트': ['P-01']
    }
    return to_excel(pd.DataFrame(sample_data))

# --- 랙 맵 ---
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
    .map-container { border: 2px solid #e0e0e0; border-radius: 10px; padding: 15px; background-color: #f9f9f9; }
    div[data-testid="column"] button { width: 100%; height: 40px !important; margin: 1px 0px !important; padding: 0px !important; font-size: 10px !important; font-weight: 700 !important; border-radius: 4px !important; border: 1px solid #ccc; }
    div[data-testid="column"] button:hover { border-color: #333 !important; transform: scale(1.05); z-index: 5; }
    button[kind="primary"] { background-color: #ffcdd2 !important; color: #b71c1c !important; border: 2px solid #d32f2f !important; }
    button[kind="secondary"] { background-color: #ffffff !important; color: #555 !important; }
    .rack-divider { border-left: 2px dashed #ddd; height: 100%; margin: 0 auto; }
    .rack-spacer { height: 30px; width: 100%; }
    .rack7-label { text-align: center; font-weight: bold; color: #555; margin-bottom: 5px; font-size: 12px; }
    </style>
    """, unsafe_allow_html=True)

    def rack_click(key):
        st.session_state.selected_rack = key
        st.session_state.filter_mode = 'rack'

    st.markdown('<div class="map-container">', unsafe_allow_html=True)
    c_left, c_mid, c_right = st.columns([3.5, 0.1, 0.8])
    
    with c_left:
        for r_num in [6]:
            cols = st.columns(7)
            for c_idx, col in enumerate(cols):
                rack_key = f"{r_num}-{c_idx+1}"
                qty = rack_summary.get(rack_key, 0)
                label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
                is_hl = (rack_key in highlight_locs) or (rack_key == st.session_state.selected_rack)
                col.button(label, key=f"btn_{rack_key}", type="primary" if is_hl else "secondary", on_click=rack_click, args=(rack_key,), use_container_width=True)
        st.markdown('<div class="rack-spacer"></div>', unsafe_allow_html=True)
        for r_num in [5, 4]:
            cols = st.columns(7)
            for c_idx, col in enumerate(cols):
                rack_key = f"{r_num}-{c_idx+1}"
                qty = rack_summary.get(rack_key, 0)
                label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
                is_hl = (rack_key in highlight_locs) or (rack_key == st.session_state.selected_rack)
                col.button(label, key=f"btn_{rack_key}", type="primary" if is_hl else "secondary", on_click=rack_click, args=(rack_key,), use_container_width=True)
        st.markdown('<div class="rack-spacer"></div>', unsafe_allow_html=True)
        for r_num in [3, 2, 1]:
            cols = st.columns(7)
            for c_idx, col in enumerate(cols):
                rack_key = f"{r_num}-{c_idx+1}"
                qty = rack_summary.get(rack_key, 0)
                label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
                is_hl = (rack_key in highlight_locs) or (rack_key == st.session_state.selected_rack)
                col.button(label, key=f"btn_{rack_key}", type="primary" if is_hl else "secondary", on_click=rack_click, args=(rack_key,), use_container_width=True)
    with c_mid: st.markdown('<div class="rack-divider"></div>', unsafe_allow_html=True)
    with c_right:
        st.markdown('<div class="rack7-label">Rack 7</div>', unsafe_allow_html=True)
        for i in range(12, 0, -1):
            rack_key = f"7-{i}"
            qty = rack_summary.get(rack_key, 0)
            label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
            is_hl = (rack_key in highlight_locs) or (rack_key == st.session_state.selected_rack)
            st.button(label, key=f"btn_{rack_key}", type="primary" if is_hl else "secondary", on_click=rack_click, args=(rack_key,), use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# --- 스캔 로직 ---
def buffer_scan():
    scan_val = str(st.session_state.scan_input).strip()
    mode = st.session_state.work_mode
    curr_loc = str(st.session_state.get('curr_location', '')).strip()
    curr_pal = str(st.session_state.get('curr_palette', '')).strip()
    
    if not scan_val: return

    df_mapping = st.session_state.df_mapping
    df_master = st.session_state.df_master
    df_log = st.session_state.df_log

    map_info = df_mapping[df_mapping['Box번호'] == scan_val]
    disp_name, disp_spec, disp_qty, p_code = "정보없음", "규격없음", 0, ""
    
    if not map_info.empty:
        p_code = str(map_info.iloc[0]['품목코드']).strip()
        disp_qty = map_info.iloc[0]['수량']
        m_info = df_master[df_master['품목코드'] == p_code]
        if not m_info.empty:
            disp_name = m_info.iloc[0]['품명']
            disp_spec = m_info.iloc[0]['규격']

    # 로그 데이터 확인 (컬럼 존재 여부 체크)
    if 'Box번호' in df_log.columns and '날짜' in df_log.columns:
        box_logs = df_log[df_log['Box번호'] == scan_val].sort_values(by='날짜', ascending=False)
        box_status, current_db_loc = "신규", "미지정"
        if not box_logs.empty:
            last_action = box_logs.iloc[0]['구분']
            current_db_loc = box_logs.iloc[0]['위치']
            if last_action in ['입고', '이동']: box_status = f"창고있음({current_db_loc})"
            elif last_action == '출고': box_status = "출고됨"
    else:
        box_status = "데이터오류"

    is_duplicate = (mode == "입고" and "창고있음" in box_status)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if mode == "조회(검색)":
        msg_text = f"🔎 조회: {scan_val} / {disp_spec} / {disp_qty}개 / {current_db_loc}"
        st.session_state.proc_msg = ("info", msg_text)
    else:
        if is_duplicate:
            st.session_state.proc_msg = ("error", f"⛔ 이미 입고됨: {scan_val}")
        else:
            final_loc = curr_loc if curr_loc else "미지정"
            final_pal = curr_pal if curr_pal else "이름없음"
            log_entry = {
                '날짜': now_str, '구분': mode, 'Box번호': scan_val,
                '품목코드': p_code, '규격': disp_spec, '수량': disp_qty,
                '위치': final_loc, '파렛트': final_pal
            }
            st.session_state.scan_buffer.append(log_entry)
            st.session_state.proc_msg = ("success", f"✅ {mode}: {scan_val}")

    st.session_state.scan_input = ""

def save_buffer_to_cloud():
    if not st.session_state.scan_buffer: return
    new_logs = pd.DataFrame(st.session_state.scan_buffer)
    if st.session_state.is_cloud:
        with st.spinner('저장 중...'):
            if save_log_data(new_logs):
                # 데이터 갱신 후 다시 로드
                st.cache_data.clear()
                st.session_state.df_log = pd.concat([st.session_state.df_log, new_logs], ignore_index=True)
                st.session_state.scan_buffer = []
                st.session_state.proc_msg = ("success", "✅ 저장 완료!")
                st.rerun()
            else: st.error("저장 실패")

def refresh_all():
    st.cache_data.clear()
    if 'data_loaded' in st.session_state: del st.session_state.data_loaded
    st.rerun()

# --- 메인 ---
def main():
    st.title("🏭 디지타스 창고 재고관리 (Ver.6.3)")
    
    if 'proc_msg' not in st.session_state: st.session_state.proc_msg = None
    if 'scan_buffer' not in st.session_state: st.session_state.scan_buffer = []
    if 'selected_rack' not in st.session_state: st.session_state.selected_rack = None
    if 'filter_mode' not in st.session_state: st.session_state.filter_mode = 'all'

    init_data()

    df_master = st.session_state.df_master
    df_mapping = st.session_state.df_mapping
    df_log = st.session_state.df_log

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

        if st.session_state.scan_buffer:
            disp_df = pd.DataFrame(st.session_state.scan_buffer)
            cols_order = ['날짜', '구분', 'Box번호', '품목코드', '규격', '수량', '위치', '파렛트']
            final_cols = [c for c in cols_order if c in disp_df.columns]
            st.dataframe(disp_df[final_cols].iloc[::-1], use_container_width=True)
        else: st.info("대기 중...")
        
        if st.button("💾 구글 시트에 저장", use_container_width=True): save_buffer_to_cloud()
        if st.button("🗑️ 목록 비우기", use_container_width=True): st.session_state.scan_buffer = []

    with tab2:
        # [강화] 필수 컬럼 체크 로직 추가
        required_cols = ['날짜', 'Box번호', '구분', '위치']
        if df_log.empty:
            st.info("데이터가 없습니다.")
        elif not set(required_cols).issubset(df_log.columns):
            st.error(f"❌ 데이터 형식 오류: 구글 시트의 [입출고] 탭 헤더가 손상되었습니다. {required_cols} 컬럼이 필요합니다.")
        else:
            try:
                last_stat = df_log.sort_values('날짜').groupby('Box번호').tail(1)
                stock_boxes = last_stat[last_stat['구분'].isin(['입고', '이동'])]
                merged = pd.merge(stock_boxes, df_mapping, on='Box번호', how='left')
                merged['위치'] = merged['위치'].fillna('미지정').replace('', '미지정')
                merged['파렛트'] = merged['파렛트'].fillna('이름없음').replace('', '이름없음')
                merged = pd.merge(merged, df_master, on='품목코드', how='left')

                d1, d2, d3 = st.columns(3)
                with d1: st.download_button("📥 재고 요약 다운로드", to_excel(merged), "재고요약.xlsx", use_container_width=True)
                with d2: st.download_button("📥 전체 상세 내역", to_excel(st.session_state.df_details), "상세내역.xlsx", use_container_width=True)
                
                st.divider()

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

                c_map, c_list = st.columns([1.5, 1])
                with c_map:
                    st.markdown("##### 🗺️ 창고 배치도")
                    render_rack_map_interactive(stock_boxes, hl_list)
                with c_list:
                    st.markdown(f"##### 📋 재고 리스트 ({len(filtered_df)}건)")
                    st.dataframe(filtered_df, use_container_width=True, height=600)
            except Exception as e: st.error(f"처리 중 오류: {e}")

    with tab3:
        st.subheader("📤 입출고 내역 일괄 업로드")
        st.download_button("📥 샘플 양식 다운로드", get_sample_file(), "입출고_샘플.xlsx")
        up = st.file_uploader("엑셀 파일", type=['xlsx', 'csv'])
        if up and st.button("구글 시트 업로드"):
            df = pd.read_excel(up) if up.name.endswith('xlsx') else pd.read_csv(up)
            if '날짜' not in df.columns: df['날짜'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for c in ['위치', '파렛트']: 
                if c not in df.columns: df[c] = ""
            with st.spinner("업로드 중..."):
                if save_data('입출고', df[['날짜', '구분', 'Box번호', '위치', '파렛트']]):
                    refresh_all()
                    st.success("완료!")

    with tab4:
        up_pack = st.file_uploader("포장 파일", type=['xlsx'])
        if up_pack and st.button("등록"):
            try:
                raw = pd.read_excel(up_pack, dtype=str)
                raw = raw.astype(str).apply(lambda x: x.str.strip())
                grp = raw.groupby(['카톤박스번호', '박스자재코드']).size().reset_index(name='수량')
                grp.columns = ['Box번호', '품목코드', '수량']
                
                dets = pd.DataFrame(columns=['Box번호', '품목코드', '규격', '압축코드'])
                if '압축코드' in raw.columns:
                    dets = raw[['카톤박스번호', '박스자재코드', '박스자재규격', '압축코드']].copy()
                    dets.columns = ['Box번호', '품목코드', '규격', '압축코드']

                items = raw[['박스자재코드', '박스자재명', '박스자재규격', '출고처명']].drop_duplicates('박스자재코드')
                items.columns = ['품목코드', '품명', '규격', '공급업체']
                items['분류구분'] = ''
                items['바코드'] = ''

                with st.spinner("등록 중..."):
                    save_data('매핑정보', grp)
                    save_data('상세내역', dets)
                    save_data('품목표', items)
                    refresh_all()
                    st.success("완료!")
            except Exception as e: st.error(f"오류: {e}")

    with tab5:
        st.dataframe(df_master)

if __name__ == '__main__':
    main()
