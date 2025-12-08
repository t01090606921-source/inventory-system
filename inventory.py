import streamlit as st
import pandas as pd
import os
import io
from datetime import datetime

# --- [1] 로그인 보안 설정 ---
def check_password():
    """로그인 성공 여부를 반환"""
    if 'password_correct' not in st.session_state:
        st.session_state.password_correct = False

    if st.session_state.password_correct:
        return True

    # 로그인 화면
    st.set_page_config(page_title="로그인", layout="centered")
    st.title("🔒 관계자 외 출입금지")
    
    pwd = st.text_input("비밀번호를 입력하세요", type="password")
    
    if st.button("로그인"):
        # 여기에 비밀번호 설정 (현재: 1234)
        # 배포 시에는 st.secrets 기능을 쓰면 더 안전합니다.
        if pwd == "1234": 
            st.session_state.password_correct = True
            st.rerun()
        else:
            st.error("비밀번호가 틀렸습니다.")
    
    return False

# --- 메인 코드 실행 전 로그인 체크 ---
if not check_password():
    st.stop() # 비밀번호 틀리면 여기서 멈춤 (아래 코드 실행 안 함)

# ==========================================
#  여기서부터는 기존 재고관리 시스템 코드입니다
# ==========================================

FILE_NAME = 'inventory_data.xlsx'

# --- 엑셀 파일 읽기/쓰기 ---
def read_excel_file():
    if not os.path.exists(FILE_NAME):
        df_m = pd.DataFrame(columns=['품목코드', '품명', '규격', '분류구분', '공급업체', '바코드'])
        df_map = pd.DataFrame(columns=['Box번호', '품목코드', '수량'])
        df_l = pd.DataFrame(columns=['날짜', '구분', 'Box번호', '위치', '파렛트'])
        df_d = pd.DataFrame(columns=['Box번호', '품목코드', '규격', '압축코드'])
        
        with pd.ExcelWriter(FILE_NAME) as writer:
            df_m.to_excel(writer, sheet_name='품목표', index=False)
            df_map.to_excel(writer, sheet_name='매핑정보', index=False)
            df_l.to_excel(writer, sheet_name='입출고', index=False)
            df_d.to_excel(writer, sheet_name='상세내역', index=False)
        return df_m, df_map, df_l, df_d
    
    try:
        df_m = pd.read_excel(FILE_NAME, sheet_name='품목표', dtype=str)
        df_map = pd.read_excel(FILE_NAME, sheet_name='매핑정보', dtype={'Box번호': str, '품목코드': str, '수량': int})
        df_l = pd.read_excel(FILE_NAME, sheet_name='입출고', dtype={'Box번호': str})
        
        if '위치' not in df_l.columns: df_l['위치'] = ""
        if '파렛트' not in df_l.columns: df_l['파렛트'] = "" 

        try:
            df_d = pd.read_excel(FILE_NAME, sheet_name='상세내역', dtype=str)
        except:
            df_d = pd.DataFrame(columns=['Box번호', '품목코드', '규격', '압축코드'])
        return df_m, df_map, df_l, df_d
    except:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def save_to_excel():
    with pd.ExcelWriter(FILE_NAME) as writer:
        st.session_state.df_master.to_excel(writer, sheet_name='품목표', index=False)
        st.session_state.df_mapping.to_excel(writer, sheet_name='매핑정보', index=False)
        st.session_state.df_log.to_excel(writer, sheet_name='입출고', index=False)
        st.session_state.df_details.to_excel(writer, sheet_name='상세내역', index=False)

def init_data():
    if 'df_master' not in st.session_state:
        m, map, l, d = read_excel_file()
        st.session_state.df_master = m
        st.session_state.df_mapping = map
        st.session_state.df_log = l
        st.session_state.df_details = d

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

# --- 랙(Rack) 배치도 그리기 ---
def render_rack_map_interactive(stock_df, highlight_locs=None):
    if highlight_locs is None: highlight_locs = []
    rack_summary = {}
    
    for _, row in stock_df.iterrows():
        raw_loc = str(row['위치']).strip()
        if not raw_loc or raw_loc == '미지정': continue
        
        parts = raw_loc.split('-')
        if len(parts) >= 3: k = f"{parts[0]}-{parts[2]}"
        elif len(parts) == 2: k = f"{parts[0]}-{parts[1]}"
        else: k = raw_loc
        
        rack_summary[k] = rack_summary.get(k, 0) + 1

    st.markdown("""
    <style>
    div[data-testid="column"] { padding: 0 2px !important; min-width: 0 !important; }
    div.stButton > button {
        width: 100%;
        height: 45px !important;
        margin: 2px 0px !important;
        padding: 0px !important;
        font-size: 10px !important;
        font-weight: 700 !important;
        border-radius: 4px !important;
        border: 1px solid #ccc;
    }
    div.stButton > button:hover { border-color: #333 !important; transform: scale(1.02); z-index: 5; }
    div.stButton > button:focus, div.stButton > button:active { background-color: #ffcdd2 !important; color: #b71c1c !important; border-color: #d32f2f !important; }
    button[kind="primary"] { background-color: #ffcdd2 !important; color: #b71c1c !important; border: 2px solid #d32f2f !important; }
    button[kind="secondary"] { background-color: #ffffff !important; color: #666 !important; }
    .rack-divider { border-left: 2px dashed #bbb; height: 100%; margin: 0 auto; }
    .rack-spacer { height: 20px; width: 100%; }
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
                is_highlight = (rack_key in highlight_locs) or (rack_key == st.session_state.selected_rack)
                btn_type = "primary" if is_highlight else "secondary"
                col.button(label, key=f"btn_{rack_key}", type=btn_type, on_click=rack_click, args=(rack_key,), use_container_width=True)
        st.markdown('<div class="rack-spacer"></div>', unsafe_allow_html=True)

        for r_num in [5, 4]:
            cols = st.columns(7)
            for c_idx, col in enumerate(cols):
                rack_key = f"{r_num}-{c_idx+1}"
                qty = rack_summary.get(rack_key, 0)
                label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
                is_highlight = (rack_key in highlight_locs) or (rack_key == st.session_state.selected_rack)
                btn_type = "primary" if is_highlight else "secondary"
                col.button(label, key=f"btn_{rack_key}", type=btn_type, on_click=rack_click, args=(rack_key,), use_container_width=True)
        st.markdown('<div class="rack-spacer"></div>', unsafe_allow_html=True)

        for r_num in [3, 2, 1]:
            cols = st.columns(7)
            for c_idx, col in enumerate(cols):
                rack_key = f"{r_num}-{c_idx+1}"
                qty = rack_summary.get(rack_key, 0)
                label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
                is_highlight = (rack_key in highlight_locs) or (rack_key == st.session_state.selected_rack)
                btn_type = "primary" if is_highlight else "secondary"
                col.button(label, key=f"btn_{rack_key}", type=btn_type, on_click=rack_click, args=(rack_key,), use_container_width=True)

    with c_mid:
        st.markdown('<div class="rack-divider"></div>', unsafe_allow_html=True)

    with c_right:
        st.markdown("**Rack 7**")
        for i in range(12, 0, -1):
            rack_key = f"7-{i}"
            qty = rack_summary.get(rack_key, 0)
            label = f"{rack_key}\n({qty})" if qty > 0 else rack_key
            is_highlight = (rack_key in highlight_locs) or (rack_key == st.session_state.selected_rack)
            btn_type = "primary" if is_highlight else "secondary"
            st.button(label, key=f"btn_{rack_key}", type=btn_type, on_click=rack_click, args=(rack_key,), use_container_width=True)

# --- 연속 스캔 처리 ---
def buffer_scan():
    scan_val = st.session_state.scan_input
    mode = st.session_state.work_mode
    curr_loc = st.session_state.get('curr_location', '').strip()
    curr_pal = st.session_state.get('curr_palette', '').strip()
    
    if not scan_val: return

    df_log = st.session_state.df_log
    df_mapping = st.session_state.df_mapping
    df_master = st.session_state.df_master

    box_logs = df_log[df_log['Box번호'] == scan_val].sort_values(by='날짜', ascending=False)
    box_status, current_db_loc = "신규", "미지정"
    
    if not box_logs.empty:
        last_action = box_logs.iloc[0]['구분']
        current_db_loc = box_logs.iloc[0]['위치'] if '위치' in box_logs.columns and pd.notna(box_logs.iloc[0]['위치']) else "미지정"
        if last_action in ['입고', '이동']: box_status = f"창고있음({current_db_loc})"
        elif last_action == '출고': box_status = "출고됨"
    
    for item in st.session_state.scan_buffer:
        if item['Box번호'] == scan_val:
            if item['구분'] in ['입고', '이동']: box_status = f"창고있음(대기중-{item['위치']})"
            elif item['구분'] == '출고': box_status = "출고됨(대기중)"

    map_info = df_mapping[df_mapping['Box번호'] == scan_val]
    disp_name, disp_qty, disp_spec = "정보없음", 0, ""
    if not map_info.empty:
        p_code = map_info.iloc[0]['품목코드']
        disp_qty = map_info.iloc[0]['수량']
        m_info = df_master[df_master['품목코드'] == p_code]
        if not m_info.empty:
            disp_name = m_info.iloc[0]['품명']
            disp_spec = m_info.iloc[0]['규격']

    msg_type, msg_text = "info", ""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if mode == "조회(검색)":
        msg_type = "info"
        msg_text = f"🔎 조회: {scan_val} | 상태: {box_status} | 규격: {disp_spec} | 수량: {disp_qty}"
        
    elif mode == "입고":
        if "창고있음" in box_status:
            msg_type = "error"; msg_text = f"⛔ 중복: Box [{scan_val}] 이미 입고됨"
        else:
            final_loc = curr_loc if curr_loc else "미지정"
            final_pal = curr_pal if curr_pal else "이름없음"
            st.session_state.scan_buffer.append({'날짜': now_str, '구분': '입고', 'Box번호': scan_val, '위치': final_loc, '파렛트': final_pal, '품명': disp_name, '규격': disp_spec, '수량': disp_qty})
            msg_type = "success"; msg_text = f"➕ 입고 대기: {disp_name} (위치:{final_loc})"

    elif mode == "재고이동":
        if "창고있음" not in box_status:
            msg_type = "error"; msg_text = f"⛔ 오류: 창고에 없는 박스입니다."
        elif not curr_loc:
            msg_type = "warning"; msg_text = "⚠️ 이동할 '적재 위치'를 입력하세요 (예: 1-2-7)"
        else:
            final_pal = curr_pal if curr_pal else "이름없음"
            st.session_state.scan_buffer.append({'날짜': now_str, '구분': '이동', 'Box번호': scan_val, '위치': curr_loc, '파렛트': final_pal, '품명': disp_name, '규격': disp_spec, '수량': disp_qty})
            msg_type = "success"; msg_text = f"🔄 이동 대기: {current_db_loc} ➔ {curr_loc}"

    elif mode == "출고":
        if "출고됨" in box_status:
            msg_type = "warning"; msg_text = f"⚠️ 이미 출고됨: Box [{scan_val}]"
        elif "신규" in box_status:
            msg_type = "error"; msg_text = f"⛔ 미입고 박스: Box [{scan_val}]"
        else:
            st.session_state.scan_buffer.append({'날짜': now_str, '구분': '출고', 'Box번호': scan_val, '위치': '', '파렛트': '', '품명': disp_name, '규격': disp_spec, '수량': disp_qty})
            msg_type = "success"; msg_text = f"➖ 출고 대기: {disp_name}"

    st.session_state.proc_msg = (msg_type, msg_text)
    st.session_state.scan_input = ""

def save_buffer():
    if not st.session_state.scan_buffer: return
    new_logs = pd.DataFrame(st.session_state.scan_buffer)[['날짜', '구분', 'Box번호', '위치', '파렛트']]
    st.session_state.df_log = pd.concat([st.session_state.df_log, new_logs], ignore_index=True)
    save_to_excel()
    st.success(f"✅ 총 {len(st.session_state.scan_buffer)}건 저장 완료!")
    st.session_state.scan_buffer = [] 
    st.session_state.proc_msg = None

def full_reset():
    st.cache_data.clear()
    m, map, l, d = read_excel_file()
    st.session_state.df_master = m
    st.session_state.df_mapping = map
    st.session_state.df_log = l
    st.session_state.df_details = d
    st.session_state.filter_mode = 'all'
    st.session_state.selected_rack = None
    if 'search_query_input' in st.session_state: del st.session_state.search_query_input

# --- 메인 화면 ---
def main():
    st.title("🏭 디지타스 창고 재고관리 시스템 (Ver.4.0)")

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

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["1. 연속 스캔", "2. 현재 재고 & 배치도", "3. 일괄 업로드", "4. 포장데이터 업로드", "5. 품목 마스터"])

    # 탭 1
    with tab1:
        c_h, c_r = st.columns([4, 1])
        with c_h: st.subheader("🚀 스캔 작업 (위치/파렛트 지정)")
        with c_r: 
            if st.button("🔄 새로고침", use_container_width=True, key='ref_1'): 
                full_reset(); st.rerun()

        if st.session_state.proc_msg:
            m_type, m_text = st.session_state.proc_msg
            if m_type == 'success': st.success(m_text)
            elif m_type == 'error': st.error(m_text)
            else: st.info(m_text)

        c1, c2, c3, c4 = st.columns([1.5, 1, 1, 2])
        with c1: st.radio("모드", ["입고", "재고이동", "출고", "조회(검색)"], horizontal=True, key="work_mode")
        with c2: st.text_input("적재 위치 (1-2-7)", key="curr_location", placeholder="랙-단-열")
        with c3: st.text_input("파렛트 이름", key="curr_palette", placeholder="선택사항")
        with c4: st.text_input("Box 번호 스캔", key="scan_input", on_change=buffer_scan)

        col_list, col_btn = st.columns([4, 1])
        with col_list:
            st.markdown(f"**대기 목록 ({len(st.session_state.scan_buffer)}건)**")
            if st.session_state.scan_buffer:
                st.dataframe(pd.DataFrame(st.session_state.scan_buffer).iloc[::-1], use_container_width=True, height=200)
            else: st.info("스캔 대기 중...")
        with col_btn:
            st.write(""); st.write("")
            if st.button("💾 일괄 저장", type="secondary", use_container_width=True, key='save_btn_tab1'): save_buffer(); st.rerun()
            if st.button("🗑️ 목록 비우기", use_container_width=True, key='clear_btn_tab1'): st.session_state.scan_buffer = []; st.session_state.proc_msg = None; st.rerun()
        
        st.divider()
        if not df_log.empty:
            full_log = pd.merge(df_log, df_mapping, on='Box번호', how='left')
            full_log = pd.merge(full_log, df_master[['품목코드', '품명', '규격']], on='품목코드', how='left')
            st.download_button("📥 전체 이력 엑셀 저장", to_excel(full_log[['날짜', '구분', '위치', '파렛트', 'Box번호', '품명', '규격', '수량']]), f"이력_{today_str}.xlsx")
            st.dataframe(full_log.sort_values(by='날짜', ascending=False).head(50)[['날짜', '구분', '위치', '파렛트', 'Box번호', '품명', '규격', '수량']], use_container_width=True)

    # 탭 2
    with tab2:
        if df_log.empty: st.info("데이터 없음")
        else:
            last_stat = df_log.sort_values('날짜').groupby('Box번호').tail(1)
            stock_boxes = last_stat[last_stat['구분'].isin(['입고', '이동'])]
            
            merged = pd.merge(stock_boxes, df_mapping, on='Box번호', how='left')
            merged['위치'] = merged['위치'].fillna('미지정').replace('', '미지정')
            merged['파렛트'] = merged['파렛트'].fillna('이름없음').replace('', '이름없음')
            merged = pd.merge(merged, df_master, on='품목코드', how='left')
            
            r_h1, r_h2 = st.columns([4, 1])
            with r_h1: st.subheader("🔍 재고 현황판")
            with r_h2: 
                if st.button("🔄 새로고침 (초기화)", use_container_width=True, key='ref_2'): 
                    full_reset(); st.rerun()

            btn1, btn2, btn3 = st.columns(3)
            
            total_sum = merged.groupby(['품목코드', '위치', '파렛트']).agg(
                현재재고=('수량', 'sum'), Box수량=('Box번호', 'count')
            ).reset_index()
            total_final = pd.merge(total_sum, df_master, on='품목코드', how='left')
            sm_cols = ['위치', '파렛트', '품목코드', '품명', '규격', '공급업체', '현재재고', 'Box수량', '분류구분', '바코드']
            
            det_all = pd.merge(stock_boxes[['Box번호', '위치', '파렛트', '날짜']], df_details, on='Box번호', how='inner')
            dt_cols = ['위치', '파렛트', 'Box번호', '품목코드', '규격', '압축코드']

            with btn1: st.download_button("📥 (전체) 재고 요약 받기", to_excel(total_final[[c for c in sm_cols if c in total_final.columns]]), f"재고요약_{today_str}.xlsx", use_container_width=True, key='down_all_sum')
            with btn2: 
                if not det_all.empty: st.download_button("📥 (전체) 상세 내역 받기", to_excel(det_all[[c for c in dt_cols if c in det_all.columns]]), f"전체상세_{today_str}.xlsx", use_container_width=True, key='down_all_det')
            
            sc1, sc2, sc3, sc4 = st.columns([1, 1, 2, 1])
            with sc1: search_target = st.selectbox("검색 기준", ["전체", "품목코드", "규격", "Box번호", "압축코드"])
            with sc2: exact_match = st.checkbox("정확히 일치", value=False)
            with sc3: search_query = st.text_input("검색어 입력", placeholder="입력 후 엔터", key="search_query_input")

            # --- 필터링 로직 ---
            filtered_df = merged.copy() 
            highlight_list = []

            if search_query:
                st.session_state.filter_mode = 'search'
                q = search_query.strip()
                
                temp_search = pd.merge(filtered_df, df_details[['Box번호', '압축코드']], on='Box번호', how='left')
                
                if search_target == "전체":
                    if exact_match:
                        mask = (temp_search['품목코드'] == q) | (temp_search['규격'] == q) | (temp_search['Box번호'] == q) | (temp_search['압축코드'] == q)
                    else:
                        mask = (temp_search['품목코드'].astype(str).str.contains(q, case=False, na=False) |
                                temp_search['규격'].astype(str).str.contains(q, case=False, na=False) |
                                temp_search['Box번호'].astype(str).str.contains(q, case=False, na=False) |
                                temp_search['압축코드'].astype(str).str.contains(q, case=False, na=False))
                else:
                    t_col = search_target if search_target != "압축코드" else "압축코드"
                    if exact_match:
                        mask = temp_search[t_col] == q
                    else:
                        mask = temp_search[t_col].astype(str).str.contains(q, case=False, na=False)
                
                matched_boxes = temp_search[mask]['Box번호'].unique()
                filtered_df = filtered_df[filtered_df['Box번호'].isin(matched_boxes)]
                
                for loc in filtered_df['위치'].unique():
                    parts = str(loc).split('-')
                    if len(parts) >= 3: highlight_list.append(f"{parts[0]}-{parts[2]}")
                    elif len(parts) == 2: highlight_list.append(f"{parts[0]}-{parts[1]}")
            
            if st.session_state.selected_rack:
                st.session_state.filter_mode = 'rack'
                sel = st.session_state.selected_rack
                highlight_list.append(sel)
                def check_loc(loc_str):
                    parts = str(loc_str).split('-')
                    if len(parts) >= 3: return f"{parts[0]}-{parts[2]}" == sel
                    elif len(parts) == 2: return f"{parts[0]}-{parts[1]}" == sel
                    return False
                
                if not search_query:
                    filtered_df = merged[merged['위치'].apply(check_loc)]
                else:
                    filtered_df = filtered_df[filtered_df['위치'].apply(check_loc)]

            with btn3:
                if not filtered_df.empty:
                    sr_df = filtered_df.groupby(['위치', '파렛트', '품목코드', '규격']).agg(
                        현재재고=('수량', 'sum'), Box수량=('Box번호', 'count')
                    ).reset_index()
                    sr_df['날짜'] = datetime.now().strftime("%Y-%m-%d")
                    cols = ['위치', '파렛트', '날짜', '품목코드', '규격', '현재재고', 'Box수량']
                    st.download_button("📥 검색/선택 결과 다운로드", to_excel(sr_df[cols]), f"검색결과_{today_str}.xlsx", use_container_width=True, key='down_search')
                else:
                    st.button("결과 없음", disabled=True, use_container_width=True, key='down_search_disabled')

            with sc4:
                if st.button("초기화", use_container_width=True, key='filter_reset'):
                    full_reset(); st.rerun()

            col_L, col_mid, col_R = st.columns([1.5, 0.1, 1])
            
            with col_L:
                st.markdown("##### 🗺️ 창고 배치도 (클릭하여 조회)")
                render_rack_map_interactive(stock_boxes, highlight_list)
            
            with col_R:
                list_title = "📋 전체 재고 리스트"
                if st.session_state.filter_mode == 'search': list_title = f"📋 검색 결과 ({len(filtered_df)}건 - Box기준)"
                elif st.session_state.filter_mode == 'rack': list_title = f"📌 [{st.session_state.selected_rack}] 상세 내용"
                
                st.markdown(f"##### {list_title}")
                if not filtered_df.empty:
                    disp_sum = filtered_df.groupby(['위치', '파렛트', '품목코드', '규격']).agg(
                        현재재고=('수량', 'sum'), Box수량=('Box번호', 'count')
                    ).reset_index()
                    st.dataframe(disp_sum, use_container_width=True, height=600)
                else:
                    st.info("결과 없음")

    with tab3:
        st.info("필수: `구분`(입고/이동/출고), `Box번호` | 선택: `위치`, `파렛트`")
        up_log = st.file_uploader("입출고 엑셀", type=['xlsx'], key="log_up")
        if up_log and st.button("적용"):
            d = pd.read_excel(up_log, dtype=str)
            if '날짜' not in d: d['날짜'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if '위치' not in d: d['위치'] = ""
            if '파렛트' not in d: d['파렛트'] = ""
            st.session_state.df_log = pd.concat([st.session_state.df_log, d[['날짜', '구분', 'Box번호', '위치', '파렛트']]], ignore_index=True)
            save_to_excel(); st.success("완료"); st.rerun()

    with tab4:
        up_pack = st.file_uploader("포장 엑셀", type=['xlsx'], key="pack_up")
        if up_pack and st.button("등록"):
            raw = pd.read_excel(up_pack, dtype=str)
            grp = raw.groupby(['카톤박스번호', '박스자재코드']).size().reset_index(name='수량')
            grp.columns = ['Box번호', '품목코드', '수량']
            existing = st.session_state.df_mapping['Box번호'].unique()
            new_map = grp[~grp['Box번호'].isin(existing)]
            if not new_map.empty:
                raw_new = raw[raw['카톤박스번호'].isin(new_map['Box번호'].unique())]
                if '압축코드' in raw_new:
                    dets = raw_new[['카톤박스번호', '박스자재코드', '박스자재규격', '압축코드']].copy()
                    dets.columns = ['Box번호', '품목코드', '규격', '압축코드']
                    st.session_state.df_details = pd.concat([st.session_state.df_details, dets], ignore_index=True)
                items = raw_new[['박스자재코드', '박스자재명', '박스자재규격', '출고처명']].drop_duplicates('박스자재코드')
                for _, r in items.iterrows():
                    if r['박스자재코드'] not in st.session_state.df_master['품목코드'].values:
                        new_item = pd.DataFrame([{'품목코드': r['박스자재코드'], '품명': r['박스자재명'], '규격': r['박스자재규격'], '공급업체': r['출고처명']}])
                        st.session_state.df_master = pd.concat([st.session_state.df_master, new_item], ignore_index=True)
                st.session_state.df_mapping = pd.concat([st.session_state.df_mapping, new_map], ignore_index=True)
                save_to_excel(); st.success("완료"); st.rerun()
            else: st.warning("데이터 없음")

    with tab5: st.dataframe(st.session_state.df_master)

if __name__ == '__main__':
    main()