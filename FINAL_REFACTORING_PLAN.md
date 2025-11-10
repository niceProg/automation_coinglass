# Final Refactoring Plan

## Problem
Current codebase has duplicate files (*_refactored.py) and needs consolidation.

## Goal
Replace original files with clean, refactored versions and remove all duplicates.

## Files to Replace

### 1. `app/models/coinglass.py`
**Action:** Merge original with refactored
- Keep ALL existing tables
- Add Table Definitions class for organization
- Maintain backward compatibility with COINGLASS_TABLES dict

### 2. `app/repositories/coinglass_repository.py`
**Action:** Replace with refactored version
- Use BaseRepository as parent
- Keep all existing methods
- Add better organization and type hints
- Maintain backward compatible method names

### 3. `app/providers/coinglass/client.py`
**Action:** Update to use BaseAPIClient
- Inherit from BaseAPIClient
- Keep all existing endpoint methods
- Add better error handling

### 4. `app/services/coinglass_service.py`
**Action:** Update imports only
- Change import paths to non-refactored files
- No functional changes

## Files to Delete (After merging)
1. `app/models/coinglass_refactored.py`
2. `app/repositories/coinglass_repository_refactored.py`
3. `app/providers/coinglass/client_refactored.py`
4. `app/providers/coinglass/pipelines/funding_rate_refactored.py`

## Files to Keep (Base classes)
1. `app/core/base_client.py` ✅
2. `app/core/base_pipeline.py` ✅
3. `app/core/base_repository.py` ✅
4. `app/core/utils.py` ✅

## Implementation Order
1. ✅ Create consolidated models/coinglass.py
2. ✅ Create consolidated coinglass_repository.py
3. ✅ Update client.py to use base_client
4. ✅ Update service imports
5. ✅ Delete all *_refactored.py files
6. ✅ Create UNUSED_FILES.md documenting what's not needed

