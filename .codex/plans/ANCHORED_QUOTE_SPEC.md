# Spec: Anchored Quote System for Precise Text Targeting

## Problem Statement

LLMs (Kimi, Claude, GPT) are incapable of accurate character-level positioning. The current system asks LLMs to provide `range.start` and `range.end` positions, resulting in 92% operation failure rate due to `snippet_mismatch` errors.

## Solution Overview

Replace character positions with **anchored quotes containing inline markers**:

```json
{
  "quoted_text": "...transformations dans les [[workscapes]] en viennent peu..."
}
```

The LLM provides:
1. **40-100 characters** total (including context and target)
2. **Double brackets `[[target]]`** around the exact span to operate on (precise targeting)

Code derives `range.start` and `range.end` via fuzzy matching + marker extraction.

**Critical constraint:** Ranges must be derived as **UTF-16 code unit offsets** (not Python string indices) because DOCX OOXML uses UTF-16.

---

## Detailed Specification

### 1. Schema Changes

**File:** `schemas/chunk_result.schema.json`

**Remove:**
```json
"range": {
  "type": "object",
  "additionalProperties": false,
  "required": ["start", "end"],
  "properties": {
    "start": { "type": "integer", "minimum": 0 },
    "end": { "type": "integer", "minimum": 0 }
  }
}
```

**Replace with:**
```json
"quoted_text": {
  "type": "string",
  "minLength": 20,
  "maxLength": 150,
  "description": "Quote 40-100 total chars with exact target wrapped in [[double brackets]]. Brackets indicate precise span; surrounding context ensures unique matching. Total length must be 40-100 characters."
}
```

**Update required fields:** Replace `"range"` with `"quoted_text"` in the ops items required array.

**Note:** The `expected.snippet` field is REMOVED. The content between `[[` and `]]` IS the expected snippet.

### 2. Prompt Changes

**File:** `templates/chunk_review.xml`

Replace the entire `<rules>` section with:

```xml
  <rules>
    <rule>Return valid JSON only, matching the provided output schema.</rule>
    <rule>Target only primary_units from the chunk payload.</rule>
    <rule>For each op, provide quoted_text (40-100 chars total) with [[target]] markers instead of character positions.</rule>
    <rule>Wrap the SPECIFIC TARGET SPAN in DOUBLE BRACKETS: [[exact words here]]</rule>
    <rule>Include 15-40 characters of context BEFORE the [[target]] and 15-40 characters AFTER.</rule>
    <rule>Total quoted_text length must be 40-100 characters including the brackets.</rule>
    <rule>Example GOOD: "transformations dans les [[workscapes]] en viennent peu" (64 chars)</rule>
    <rule>Example BAD (too short): "les [[workscapes]] en" (23 chars - not enough context)</rule>
    <rule>Example BAD (no brackets): "transformations dans les workscapes en viennent"</rule>
    <rule>Context proves WHICH occurrence you mean; brackets show EXACTLY which words to target.</rule>
    <rule>If target appears multiple times, use MORE context to disambiguate.</rule>
    <rule>For type=replace_range: replacement field is what replaces the [[target]] content.</rule>
    <rule>For type=insert_at: Use [[ ]] (space between brackets) at insertion point: "before [[ ]]after"</rule>
    <rule>For type=add_comment: Brackets indicate which span receives the comment.</rule>
    <rule>The [[ and ]] are positioning markers ONLY—never include them in your replacement or thinking.</rule>
    <rule>Do NOT use markdown, code blocks, or extra formatting in quoted_text.</rule>
    <rule>If target is at start: "[[Target]] at beginning with context after"</rule>
    <rule>If target is at end: "Context before ending with [[target]]"</rule>
    <rule>For unused fields (replacement, new_text, comment_text depending on op type), set empty string "".</rule>
  </rules>
```

### 3. Code Changes: Range Derivation Logic

**File:** `scripts/run_project.py`

**New Functions to Add:**

```python
import re
import unicodedata
from difflib import SequenceMatcher
from dataclasses import dataclass

@dataclass
class MatchResult:
    start: int  # UTF-16 code unit offset
    end: int    # UTF-16 code unit offset
    score: float


def _derive_range_from_quoted_text(
    quoted_text: str, 
    unit_text: str, 
    op_index: int,
    chunk_id: str = ""
) -> tuple[int, int]:
    """
    Derive UTF-16 character range from quoted text with inline markers.
    
    Args:
        quoted_text: Text with [[target]] markers
        unit_text: Full text of the target unit (Unicode string)
        op_index: Index for error reporting
        chunk_id: Chunk ID for error reporting
        
    Returns:
        (start, end) UTF-16 code unit positions (not Python string indices)
        
    Raises:
        RuntimeError: If quoted_text is invalid or cannot be found
    """
    # Validate brackets exist and are properly paired
    if quoted_text.count("[[") != 1 or quoted_text.count("]]") != 1:
        raise RuntimeError(
            f"Chunk {chunk_id} op {op_index}: quoted_text must contain exactly one [[ and one ]]. "
            f"Got: {quoted_text[:50]}..."
        )
    
    # Use regex to extract components (handles nested brackets gracefully)
    # Pattern: everything up to [[, then target, then everything after ]]
    match = re.match(r'^(.*?)\[\[(.*?)\]\](.*)$', quoted_text, re.DOTALL)
    if not match:
        raise RuntimeError(
            f"Chunk {chunk_id} op {op_index}: Cannot parse [[markers]] in quoted_text. "
            f"Got: {quoted_text[:50]}..."
        )
    
    pre, target, post = match.groups()
    
    # Handle insert-at case: collapsed markers [[ ]] or [[]]
    is_insert_at = target.strip() == ""
    
    # Build search text (quote without brackets)
    # Normalize Unicode to NFC for matching
    search_text = unicodedata.normalize('NFC', pre + target + post)
    unit_text_nfc = unicodedata.normalize('NFC', unit_text)
    
    # Fuzzy find
    match_result = _fuzzy_find(search_text, unit_text_nfc, threshold=0.80)
    if not match_result:
        # Try with whitespace normalized
        search_normalized = re.sub(r'\s+', ' ', search_text).strip()
        unit_normalized = re.sub(r'\s+', ' ', unit_text_nfc)
        match_result = _fuzzy_find(search_normalized, unit_normalized, threshold=0.85)
        
    if not match_result:
        raise RuntimeError(
            f"Chunk {chunk_id} op {op_index}: Cannot find quoted_text in unit.\n"
            f"  Quoted: {quoted_text[:80]}...\n"
            f"  Target: {target[:40] if target else '(insert)'}"
        )
    
    # Calculate positions in Unicode codepoints first
    match_start_cp = match_result.start
    
    # Adjust for pre-context to get actual target start
    # Normalize pre the same way we normalized search_text
    pre_normalized = unicodedata.normalize('NFC', pre)
    if pre_normalized:
        # Find pre_normalized within the matched text
        # Since we fuzzy-matched, we need to find where pre ends in the match
        matched_text = unit_text_nfc[match_start_cp:match_start_cp + len(search_text)]
        # Find where target starts within the matched text
        target_idx_in_match = matched_text.find(target) if target else len(pre_normalized)
        if target_idx_in_match < 0:
            target_idx_in_match = len(pre_normalized)  # Fallback
    else:
        target_idx_in_match = 0
    
    target_start_cp = match_start_cp + target_idx_in_match
    target_end_cp = target_start_cp + len(target) if target else target_start_cp
    
    # Convert to UTF-16 code units for DOCX compatibility
    text_before_start = unit_text_nfc[:target_start_cp]
    text_before_end = unit_text_nfc[:target_end_cp]
    
    start_utf16 = len(text_before_start.encode('utf-16-le')) // 2
    end_utf16 = len(text_before_end.encode('utf-16-le')) // 2
    
    # Validate the derived range
    # (Optional: verify by extracting from unit_text using UTF-16 indices)
    
    return start_utf16, end_utf16


def _fuzzy_find(needle: str, haystack: str, threshold: float = 0.85) -> MatchResult | None:
    """
    Find needle in haystack with fuzzy matching.
    
    Uses SequenceMatcher for similarity. Optimized with early-exit for exact matches.
    Returns MatchResult with UTF-16 positions, or None if no match above threshold.
    """
    # Early exit: exact match
    idx = haystack.find(needle)
    if idx >= 0:
        # Calculate UTF-16 position
        text_before = haystack[:idx]
        start_utf16 = len(text_before.encode('utf-16-le')) // 2
        end_utf16 = start_utf16 + len(needle.encode('utf-16-le')) // 2
        return MatchResult(start=start_utf16, end=end_utf16, score=1.0)
    
    # For short needles, sliding window is fast enough
    # For long needles, use larger step or Boyer-Moore pre-filter
    needle_len = len(needle)
    haystack_len = len(haystack)
    
    if needle_len > haystack_len:
        return None
    
    # Optimization: if needle is long (>200 chars), check fewer windows
    step = 1 if needle_len < 100 else 5
    
    best_score = 0.0
    best_start = -1
    
    for i in range(0, haystack_len - needle_len + 1, step):
        window = haystack[i:i + needle_len]
        score = SequenceMatcher(None, needle, window).ratio()
        if score > best_score:
            best_score = score
            best_start = i
            if score == 1.0:  # Exact match found
                break
    
    if best_score >= threshold:
        # Fine-tune around best match (in case we skipped with step > 1)
        if step > 1:
            start_fine = max(0, best_start - step)
            end_fine = min(haystack_len - needle_len + 1, best_start + step + 1)
            for i in range(start_fine, end_fine):
                window = haystack[i:i + needle_len]
                score = SequenceMatcher(None, needle, window).ratio()
                if score > best_score:
                    best_score = score
                    best_start = i
        
        text_before = haystack[:best_start]
        start_utf16 = len(text_before.encode('utf-16-le')) // 2
        end_utf16 = start_utf16 + len(needle.encode('utf-16-le')) // 2
        return MatchResult(start=start_utf16, end=end_utf16, score=best_score)
    
    return None
```

**Integration in `_sanitize_chunk_result_ops()`:**

Replace the current range validation logic with:

```python
# OLD: Validate and use provided range
# range_obj = raw_op.get("range")
# normalized_range = _normalize_range(range_obj)

# NEW: Derive range from quoted_text
quoted_text = raw_op.get("quoted_text", "")
if not quoted_text:
    raise RuntimeError(f"Chunk {chunk_id} op {idx}: missing quoted_text")

# Get unit text from primary_units lookup
unit_text = ...  # Get from unit_uid_to_target mapping

try:
    start_utf16, end_utf16 = _derive_range_from_quoted_text(
        quoted_text, unit_text, idx, chunk_id
    )
    normalized_range = {"start": start_utf16, "end": end_utf16}
except RuntimeError as e:
    raise RuntimeError(f"Failed to derive range: {e}")
```

### 4. Edge Cases & Handling

| Edge Case | Detection | Handling |
|-----------|-----------|----------|
| Missing brackets | `quoted_text.count("[[") == 0` | RuntimeError: "quoted_text must contain [[target]]" |
| Unmatched brackets | `quoted_text.count("[[") != quoted_text.count("]]")` | RuntimeError: "Unmatched brackets" |
| Multiple bracket pairs | `quoted_text.count("[[") > 1` | RuntimeError: "Only one [[target]] allowed" |
| Nested brackets in target | Regex handles via non-greedy match | Correctly extracts innermost or first valid pair |
| Empty target (insert-at) | `target.strip() == ""` | Valid: `range.start == range.end` |
| Quote too short (<40) | `len(quoted_text) < 40` | Warning logged, still attempt match |
| Quote too long (>100) | `len(quoted_text) > 100` | Warning logged, truncate to 100 or error |
| No fuzzy match | `_fuzzy_find` returns None | RuntimeError with context snippet |
| Multiple close matches | Multiple scores > threshold | Pick highest, log warning with alternatives |
| Unicode mismatch (NFC/NFD) | Normalize both to NFC before matching | Automatic handling in `_derive_range` |
| Whitespace differences | Collapse multiple spaces | Try normalized whitespace match as fallback |
| Markdown in quote | `"**[[target]]**"` | Clean by removing common markdown patterns |
| Target not found after match | Validation fails | RuntimeError: "Position derivation failed" |

### 5. Validation & Error Messages

All errors must include:
- Chunk ID and operation index
- The quoted_text that failed
- The target text (content between brackets)
- The match score if fuzzy matching was attempted
- Hint for how to fix

Example errors:
```
RuntimeError: Chunk chunk_0001 op 3: Cannot locate quoted_text.
  Quoted: "transformations dans les [[workscapes]]"
  Target: "workscapes"
  Match score: 0.72 (threshold: 0.80)
  Hint: Ensure quoted_text appears exactly in the document. Try including more unique context.
```

### 6. Testing Requirements

**Unit tests for `_derive_range_from_quoted_text`:**
1. ✅ Exact match with standard ASCII
2. ✅ French accents (café, résumé) - verify NFC normalization
3. ✅ Emojis/multi-byte UTF-8 - verify UTF-16 position calculation
4. ✅ Insert-at with `[[ ]]` - verify collapsed range
5. ✅ Insert-at with `[[]]` - verify collapsed range
6. ✅ Missing brackets raises RuntimeError
7. ✅ Unmatched brackets raises RuntimeError
8. ✅ Multiple bracket pairs raises RuntimeError
9. ✅ Target not found raises RuntimeError
10. ✅ Fuzzy match with 1-char typo succeeds
11. ✅ Fuzzy match with 5-char difference fails (below threshold)
12. ✅ Multiple spaces normalized and matched
13. ✅ Newlines in quoted text handled

**Unit tests for `_fuzzy_find`:**
1. ✅ Exact match returns score 1.0
2. ✅ Typo tolerance ("workscape" matches "workscapes" at ~0.95)
3. ✅ Threshold filtering (0.79 match rejected at 0.80 threshold)
4. ✅ Empty needle returns None
5. ✅ Needle longer than haystack returns None
6. ✅ Unicode characters handled correctly
7. ✅ Performance on 10k char text completes in <100ms

**Integration tests:**
1. Run full pipeline on sample chunk
2. Verify 0% `snippet_mismatch` in apply_log
3. Visual inspection: comments appear on correct words in output DOCX
4. Verify replace_range operations target correct spans
5. Verify insert_at operations insert at correct positions

### 7. Migration Path

**Breaking Changes:**
1. Schema: `range` object removed, `quoted_text` string added
2. Schema: `expected.snippet` removed (redundant with [[target]])
3. Prompt: Complete rewrite of `<rules>` section
4. Logic: Range derivation moved from LLM to code

**Migration Steps:**
1. ✅ Backup existing chunk results (they're incompatible)
2. ✅ Update schema file
3. ✅ Update prompt template
4. ✅ Implement new derivation functions
5. ✅ Update `_sanitize_chunk_result_ops` integration
6. ✅ Run unit tests
7. ✅ Clear `projects/thesis/artifacts/chunk_results/`
8. ✅ Run full pipeline test
9. ✅ Verify output DOCX

**Schema Version:** Bump schema version from `1.0.0` to `2.0.0` to indicate breaking change.

---

## Success Criteria

1. **Schema validation:** Kimi/Claude/Codex outputs pass new schema
2. **Accuracy:** >95% of ops successfully derive valid ranges (vs current 8%)
3. **Precision:** Comments appear on correct words (visual inspection of DOCX)
4. **Robustness:** Handles minor LLM transcription errors (fuzzy matching at 0.80 threshold)
5. **Performance:** Fuzzy matching on longest unit (<5000 chars) completes in <200ms
6. **Zero tolerance:** 0% `snippet_mismatch` rate in final output

---

## Implementation Notes

### Performance Optimization

For very long review units (5000+ characters), the O(n²) sliding window approach may be slow. If profiling reveals this as a bottleneck:

1. **Pre-filter:** Use `haystack.find(needle[:20])` to find candidate regions
2. **Window only around candidates:** Only run SequenceMatcher on ±100 char windows
3. **Consider rapidfuzz:** If acceptable to add dependency, `rapidfuzz.fuzz.partial_ratio` is 10-100x faster

### Unicode Handling

DOCX uses UTF-16 internally. Python strings use Unicode codepoints. The conversion:

```python
# Python codepoint index → UTF-16 offset
text_before = unit_text[:codepoint_index]
utf16_offset = len(text_before.encode('utf-16-le')) // 2
```

This correctly handles:
- Basic Multilingual Plane (BMP): 1 codepoint = 1 UTF-16 unit
- Astral plane (emojis, rare CJK): 1 codepoint = 2 UTF-16 units (surrogate pairs)

### Debug Logging

Add verbose logging during development:

```python
_log_line(f"[derive] quoted: {quoted_text[:50]}...")
_log_line(f"[derive] match score: {match_result.score:.2f}")
_log_line(f"[derive] derived range: [{start_utf16}:{end_utf16}]")
```

Remove or demote to debug level after validation.
