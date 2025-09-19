import os
import json
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import multiprocessing
from tqdm import tqdm


# ========== 切分函數 ==========
def split_text_to_sentences(text, min_length=30, max_length=250):
    sentence_separators = ['。', '！', '？', '；', '…']
    phrase_separators = ['，', '、', '：', '；']
    sentences, current_sentence = [], ""
    for char in text:
        current_sentence += char
        if char in sentence_separators:
            if current_sentence.strip():
                sentences.append(current_sentence.strip())
                current_sentence = ""
    if current_sentence.strip():
        sentences.append(current_sentence.strip())

    fragments = []
    for sentence in sentences:
        if len(sentence) < min_length:
            continue
        if len(sentence) <= max_length:
            fragments.append(sentence)
        else:
            parts, current_part = [], ""
            for char in sentence:
                current_part += char
                if char in phrase_separators:
                    if current_part.strip() and len(current_part.strip()) >= min_length:
                        parts.append(current_part.strip())
                        current_part = ""
            if current_part.strip() and len(current_part.strip()) >= min_length:
                parts.append(current_part.strip())

            merged_parts, temp_part = [], ""
            for part in parts:
                if len(temp_part + part) <= max_length:
                    temp_part = temp_part + part if temp_part else part
                else:
                    if temp_part and len(temp_part) >= min_length:
                        merged_parts.append(temp_part)
                    temp_part = part
            if temp_part and len(temp_part) >= min_length:
                merged_parts.append(temp_part)

            if not merged_parts and len(sentence) >= min_length:
                for i in range(0, len(sentence), max_length):
                    fragment = sentence[i:i + max_length]
                    if len(fragment) >= min_length:
                        merged_parts.append(fragment)
            fragments.extend(merged_parts)
    return fragments


# ========== 處理單筆 ==========
def process_single_row(args):
    idx, row_dict, min_length, max_length = args
    original_text = row_dict.get('text', '')
    if not isinstance(original_text, str) or len(original_text.strip()) < min_length:
        return []

    fragments = split_text_to_sentences(original_text, min_length, max_length)
    results = []
    for frag_idx, fragment in enumerate(fragments):
        start_pos = original_text.find(fragment)
        end_pos = start_pos + len(fragment)
        result = {
            **row_dict,
            'text': fragment,
            'original_index': idx,
            'fragment_index': frag_idx,
            'original_text_length': len(original_text),
            'fragment_length': len(fragment),
            'source_type': 'sentence_fragment',
            'fragment_start': start_pos,
            'fragment_end': end_pos
        }
        results.append(result)
    return results


# ========== 主流程 ==========
def process_in_parallel(df, chunk_id, min_length=30, max_length=250, num_workers=None):
    print(f"🚀 處理 Chunk {chunk_id}，筆數：{len(df)}")
    args_list = [(idx, row, min_length, max_length) for idx, row in enumerate(df.to_dict('records'))]

    results = []
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        for result in tqdm(executor.map(process_single_row, args_list), total=len(args_list)):
            if result:
                results.extend(result)

    out_file = f"split_partial_chunk_{chunk_id}.csv"
    pd.DataFrame(results).to_csv(out_file, index=False, encoding='utf-8-sig')
    print(f"💾 Chunk {chunk_id} 完成，儲存至 {out_file}")
    return out_file


# ========== 入口點 ==========
def main():
    # -------- 設定參數 --------
    input_file = "/Users/liuchunwen/Downloads/test_cluecorpus.csv"
    text_column = "text"
    min_length = 30
    max_length = 250
    chunk_size = 100_000  # 每批處理筆數（可調）
    num_workers = max(1, multiprocessing.cpu_count() - 1)  # 自動偵測核心數

    print(f"🔍 載入資料：{input_file}")
    df = pd.read_csv(input_file, encoding='utf-8-sig')

    # -------- 分批處理 --------
    total_chunks = (len(df) + chunk_size - 1) // chunk_size
    output_files = []

    for chunk_id in range(total_chunks):
        start = chunk_id * chunk_size
        end = min(start + chunk_size, len(df))
        df_chunk = df.iloc[start:end].copy()
        output_file = process_in_parallel(df_chunk, chunk_id, min_length, max_length, num_workers)
        output_files.append(output_file)

    # -------- 合併結果 --------
    print("🧩 合併所有 chunk 結果...")
    all_dfs = [pd.read_csv(f) for f in output_files]
    final_df = pd.concat(all_dfs, ignore_index=True)

    # -------- 輸出結果 --------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "split_output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"sentence_fragments_{timestamp}.csv")
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    print(f"\n✅ 全部完成，已儲存至：{output_path}")
    print(f"📊 總片段數：{len(final_df)}")
    print("🎉")


# ========== 執行 ==========
if __name__ == "__main__":
    main()
