#!/usr/bin/env python3
"""
Memory-efficient script to combine, clean, and process large text files.
Removes numbers and unwanted characters, keeping only letters and .,!? punctuation.
Uses streaming to handle multi-gigabyte files without memory issues.
Usage: python combine_and_clean_v2.py [folder_path] [output_file]
"""

import os
import sys
import re
import time
import tempfile
import json
from pathlib import Path

def progress_bar(current, total, bar_length=50, prefix="Progress", start_time=None):
    """Display a progress bar in the terminal with ETA."""
    if total == 0:
        return
    
    percent = float(current) * 100 / total
    filled_length = int(bar_length * current // total)
    bar = '█' * filled_length + '░' * (bar_length - filled_length)
    
    # Calculate ETA
    eta_str = ""
    if start_time and current > 0:
        elapsed = time.time() - start_time
        rate = current / elapsed
        if rate > 0:
            remaining = (total - current) / rate
            if remaining < 60:
                eta_str = f" ETA: {int(remaining)}s"
            elif remaining < 3600:
                eta_str = f" ETA: {int(remaining/60)}m {int(remaining%60)}s"
            else:
                eta_str = f" ETA: {int(remaining/3600)}h {int((remaining%3600)/60)}m"
    
    # Build output string and ensure it's not longer than 120 chars to prevent wrapping
    output = f'{prefix}: |{bar}| {percent:.1f}% ({current}/{total}){eta_str}'
    output = output[:120]  # Truncate if too long
    
    # Carriage return to beginning of line, write output, clear to end of line
    sys.stdout.write(f'\r{output}')
    sys.stdout.write('\033[K')  # Clear from cursor to end of line
    sys.stdout.flush()
    
    if current == total:
        sys.stdout.write('\n')
        sys.stdout.flush()

def extract_text_from_json(json_file):
    """
    Extract ONLY the actual poem text from JSON file.
    Ignores all linguistic annotations (lemma, upos, xpos, feats, deprel, etc.)
    Only extracts the 'text' field from each line in the 'body' array.
    """
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract only the 'text' field from each line in the body
        texts = []
        if 'body' in data and isinstance(data['body'], list):
            for line in data['body']:
                if isinstance(line, dict) and 'text' in line:
                    texts.append(line['text'])
        
        # Join with newlines to preserve line structure
        return '\n'.join(texts) if texts else ""
    except Exception as e:
        print(f"\n⚠️  Warning: Could not parse JSON from {json_file.name}: {e}")
        return ""

def combine_files_streaming(files, output_file, show_progress=True):
    """
    Combine files (txt and json) in streaming mode and delete as processed.
    Returns: (deleted_count, total_bytes)
    """
    if show_progress:
        print("📚 Combining & deleting files (streaming)...")
    
    deleted_count = 0
    total_bytes = 0
    chunk_size = 1024 * 1024  # 1MB chunks
    start_time = time.time()
    last_update_time = start_time
    
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for i, file_path in enumerate(files):
            try:
                # Handle JSON files differently
                if file_path.suffix.lower() == '.json':
                    text = extract_text_from_json(file_path)
                    if text:
                        total_bytes += len(text.encode('utf-8'))
                        outfile.write(text)
                        outfile.write('\n')
                else:
                    # Stream text file content in chunks
                    with open(file_path, 'r', encoding='utf-8') as infile:
                        while True:
                            chunk = infile.read(chunk_size)
                            if not chunk:
                                break
                            total_bytes += len(chunk.encode('utf-8'))
                            outfile.write(chunk)
                        outfile.write('\n')
                
                # Delete immediately after reading
                file_path.unlink()
                deleted_count += 1
                
                # Update progress throttled by time (every second) or at completion
                if show_progress:
                    current_time = time.time()
                    if (current_time - last_update_time >= 1.0) or (i + 1 == len(files)):
                        progress_bar(i + 1, len(files), prefix="Reading & deleting", start_time=start_time)
                        last_update_time = current_time
                        
            except Exception as e:
                # Clear progress bar before printing error
                sys.stdout.write('\r')
                sys.stdout.write('\033[K')
                sys.stdout.flush()
                print(f"⚠️  Warning: Could not process {file_path.name}: {e}")
                continue
    
    return deleted_count, total_bytes

def clean_text_streaming(input_file, output_file, show_progress=True):
    """
    Clean text keeping only letters, spaces, and .,!? punctuation.
    Removes duplicate words that appear consecutively.
    Processes in chunks to handle large files.
    Returns: (original_chars, cleaned_chars)
    """
    if show_progress:
        print("🧹 Cleaning text (streaming)...")
    
    file_size = input_file.stat().st_size
    bytes_processed = 0
    original_chars = 0
    cleaned_chars = 0
    chunk_size = 1024 * 1024  # 1MB chunks
    start_time = time.time()
    last_update_time = start_time
    
    # Since we're now extracting ONLY the 'text' field from JSON (not annotations),
    # we just need to remove any remaining metadata artifacts
    metadata_patterns = [
        # Source/metadata identifiers that might appear in text
        r'\b(gutenberg)\b',
    ]
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        while True:
            chunk = infile.read(chunk_size)
            if not chunk:
                break
            
            original_chars += len(chunk)
            bytes_processed += len(chunk.encode('utf-8'))
            
            # Remove metadata patterns (IMPORTANT - this removes the linguistic tags!)
            for pattern in metadata_patterns:
                chunk = re.sub(pattern, '', chunk)
            
            # Keep only allowed characters (IMPORTANT - removes numbers and special chars!)
            cleaned_chunk = re.sub(r'[^a-zA-Z\s.,!?\n]', '', chunk)
            
            # NEW: Remove consecutive duplicate words
            # Split into tokens, remove duplicates, rejoin
            lines = cleaned_chunk.split('\n')
            deduplicated_lines = []
            
            for line in lines:
                tokens = line.split()
                dedup_tokens = []
                prev_token = None
                
                for token in tokens:
                    token_lower = token.lower()
                    # Skip if it's the same as previous token
                    if prev_token and token_lower == prev_token.lower():
                        continue
                    dedup_tokens.append(token)
                    prev_token = token
                
                if dedup_tokens:
                    deduplicated_lines.append(' '.join(dedup_tokens))
                else:
                    deduplicated_lines.append('')
            
            cleaned_chunk = '\n'.join(deduplicated_lines)
            
            # Clean up multiple spaces (IMPORTANT!)
            cleaned_chunk = re.sub(r' +', ' ', cleaned_chunk)
            
            cleaned_chars += len(cleaned_chunk)
            outfile.write(cleaned_chunk)
            
            # Update progress throttled by time (every second)
            if show_progress and file_size > 0:
                current_time = time.time()
                progress = min(100, int(bytes_processed / file_size * 100))
                if (current_time - last_update_time >= 1.0) or (progress == 100):
                    progress_bar(progress, 100, prefix="Cleaning", start_time=start_time)
                    last_update_time = current_time
    
    return original_chars, cleaned_chars

def process_lines_streaming(input_file, output_file, show_progress=True, max_word_repeats=3):
    """
    Remove single-word lines, duplicates, and lines with excessive word repetition.
    Returns: (removed_single_word, duplicate_count, removed_repeats, total_lines)
    """
    if show_progress:
        print("🔍 Filtering & deduplicating (streaming)...")
    
    file_size = input_file.stat().st_size
    seen_lines = set()
    removed_single_word = 0
    duplicate_count = 0
    removed_repeats = 0
    total_lines = 0
    bytes_processed = 0
    consecutive_empty = 0
    start_time = time.time()
    last_update_time = start_time
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            total_lines += 1
            bytes_processed += len(line.encode('utf-8'))
            
            # Update progress throttled by time (every second)
            if show_progress and file_size > 0:
                current_time = time.time()
                progress = min(100, int(bytes_processed / file_size * 100))
                if (current_time - last_update_time >= 1.0) or (progress == 100):
                    progress_bar(progress, 100, prefix="Processing", start_time=start_time)
                    last_update_time = current_time
            
            line = line.strip()
            
            # Handle empty lines - limit consecutive empties
            if not line:
                consecutive_empty += 1
                if consecutive_empty <= 2:
                    outfile.write('\n')
                continue
            else:
                consecutive_empty = 0
            
            # Split into words
            words = line.split()
            
            # Skip single-word lines
            if len(words) <= 1:
                removed_single_word += 1
                continue
            
            # Check for lines that are mostly the same word repeated
            # Only flag if 80%+ of the words are the same word
            if len(words) >= 5:  # Only check lines with 5+ words
                word_counts = {}
                for word in words:
                    word_lower = word.lower()
                    word_counts[word_lower] = word_counts.get(word_lower, 0) + 1
                
                # If any single word makes up more than 80% of the line, it's spam
                max_count = max(word_counts.values())
                if max_count / len(words) > 0.8:
                    removed_repeats += 1
                    continue
            
            # Skip duplicates
            if line in seen_lines:
                duplicate_count += 1
                continue
            
            # Write unique multi-word line
            seen_lines.add(line)
            outfile.write(line + '\n')
    
    return removed_single_word, duplicate_count, removed_repeats, total_lines

def find_files_recursive(folder_path):
    """
    Recursively find all .txt and .json files in folder and subfolders.
    Returns list of Path objects.
    """
    folder = Path(folder_path)
    files = []
    
    # Find .txt files recursively
    files.extend(folder.rglob("*.txt"))
    
    # Find .json files recursively
    files.extend(folder.rglob("*.json"))
    
    return sorted(files)

def combine_and_clean_files(folder_path=".", output_file="combined_cleaned.txt"):
    """
    Main function to combine, clean, and process all text/json files recursively.
    Uses streaming to handle files of any size.
    """
    folder = Path(folder_path)
    
    if not folder.exists():
        print(f"❌ Error: Folder '{folder_path}' does not exist.")
        return False
    
    if not folder.is_dir():
        print(f"❌ Error: '{folder_path}' is not a directory.")
        return False
    
    # Find all .txt and .json files recursively
    print("🔍 Scanning for files recursively...")
    all_files = find_files_recursive(folder_path)
    
    if not all_files:
        print(f"❌ No .txt or .json files found in '{folder_path}' or its subdirectories")
        return False
    
    # Group by file type for display
    txt_files = [f for f in all_files if f.suffix.lower() == '.txt']
    json_files = [f for f in all_files if f.suffix.lower() == '.json']
    
    print(f"\n📁 Found {len(all_files)} files total:")
    print(f"  📄 {len(txt_files)} text files")
    print(f"  📋 {len(json_files)} JSON files")
    print(f"\nFiles from folders:")
    
    # Show folder structure
    folders = set(f.parent for f in all_files)
    for folder_item in sorted(folders):
        folder_files = [f for f in all_files if f.parent == folder_item]
        print(f"  📂 {folder_item.relative_to(folder)}/  ({len(folder_files)} files)")
    
    # Confirm with user
    print(f"\n🎯 Will combine into '{output_file}', clean, and DELETE originals as processed.")
    print("⚠️  WARNING: Original files will be DELETED as they are read!")
    print("💾 Using streaming mode - can handle multi-GB files!")
    print("🔄 Processing recursively through all subdirectories!")
    
    response = input("\nProceed? (y/N): ")
    if response.lower() != 'y':
        print("❌ Operation cancelled.")
        return False
    
    output_path = folder / output_file
    
    try:
        # Create temporary directory for intermediate files
        temp_dir = tempfile.mkdtemp()
        temp_combined = Path(temp_dir) / "temp_combined.txt"
        temp_cleaned = Path(temp_dir) / "temp_cleaned.txt"
        
        # STEP 1: Combine files
        print("\n" + "=" * 60)
        print("📚 STEP 1: Combining & deleting files...")
        print("=" * 60)
        
        deleted_count, total_bytes = combine_files_streaming(all_files, temp_combined, show_progress=True)
        
        print(f"✅ Combined {len(txt_files)} files")
        print(f"📏 Total size: ~{total_bytes:,} bytes")
        
        # STEP 2: Clean text
        print("\n" + "=" * 60)
        print("🧼 STEP 2: Cleaning text...")
        print("=" * 60)
        
        original_chars, cleaned_chars = clean_text_streaming(temp_combined, temp_cleaned, show_progress=True)
        removed_chars = original_chars - cleaned_chars
        
        print(f"\n📊 Cleaning Statistics:")
        print(f"  📏 Original: {original_chars:,} characters")
        print(f"  🧹 Cleaned:  {cleaned_chars:,} characters")
        print(f"  🗑️  Removed:  {removed_chars:,} characters ({removed_chars/original_chars*100:.1f}%)")
        
        # Free up space
        temp_combined.unlink()
        
        # STEP 3: Process lines (filter & deduplicate)
        print("\n" + "=" * 60)
        print("🔍 STEP 3: Filtering & deduplicating...")
        print("=" * 60)
        
        removed_lines, duplicate_count, removed_repeats, total_lines = process_lines_streaming(temp_cleaned, output_path, show_progress=True)
        
        print(f"\n📊 Line Processing Statistics:")
        print(f"  📝 Total lines: {total_lines:,}")
        print(f"  🗑️  Removed single-word lines: {removed_lines:,}")
        print(f"  🔄 Removed duplicates: {duplicate_count:,}")
        print(f"  🔁 Removed lines with excessive repetition: {removed_repeats:,}")
        
        # Clean up
        temp_cleaned.unlink()
        try:
            os.rmdir(temp_dir)
        except:
            pass
        
        # Final stats
        final_size = output_path.stat().st_size
        
        print("\n" + "=" * 60)
        print("🎉 ALL DONE!")
        print("=" * 60)
        print(f"✅ Combined {len(all_files)} files ({len(txt_files)} txt, {len(json_files)} json)")
        print(f"✅ Deleted {deleted_count}/{len(all_files)} original files")
        print(f"✅ Removed {removed_chars:,} unwanted characters")
        print(f"✅ Removed {removed_lines:,} single-word lines")
        print(f"✅ Removed {duplicate_count:,} duplicate lines")
        print(f"✅ Removed {removed_repeats:,} lines with excessive word repetition")
        print(f"✅ Final file size: {final_size:,} bytes")
        print(f"✅ Saved to: {output_path.name}")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main entry point."""
    
    if len(sys.argv) == 1:
        folder_path = "."
        output_file = "combined_cleaned.txt"
    elif len(sys.argv) == 2:
        folder_path = sys.argv[1]
        output_file = "combined_cleaned.txt"
    elif len(sys.argv) >= 3:
        folder_path = sys.argv[1]
        output_file = sys.argv[2]
    else:
        print("Usage: python combine_and_clean_v2.py [folder_path] [output_file]")
        print("\n💾 Memory-efficient streaming mode - handles multi-GB files!")
        print("🔄 Recursively processes all subdirectories!")
        print("📋 Supports both .txt and .json files!")
        print("\nArguments:")
        print("  folder_path    Folder with .txt/.json files (default: current directory)")
        print("  output_file    Output filename (default: combined_cleaned.txt)")
        print("\n⚠️  Original files are DELETED as they're processed!")
        print("\nFeatures:")
        print("  • Recursively scans all subdirectories")
        print("  • Handles .txt and .json files")
        print("  • Extracts text from JSON automatically")
        print("  • Removes numbers and special characters")
        print("  • Keeps only letters and .,!? punctuation")
        print("  • Filters single-word lines")
        print("  • Removes duplicate lines")
        sys.exit(1)
    
    success = combine_and_clean_files(folder_path, output_file)
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
