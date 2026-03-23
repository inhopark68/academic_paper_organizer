# academic-paper-organizer

학술논문 PDF를 자동 감시해서 DOI를 추출하고, 메타데이터를 조회해 파일명을 바꾸고 폴더별로 정리하는 Python 패키지입니다.

이 버전에는 **검색 인덱스(SQLite)** 가 포함되어 있어서, 분류된 PDF를 키워드로 찾을 수 있습니다.

## 설치

```bash
pip install .
```

개발 모드 설치:

```bash
pip install -e .
```

## 기본 실행

기존 방식 그대로 실행할 수 있습니다.

```bash
paper-organizer --watch "C:/Users/USER/Downloads" --output "D:/Papers"
```

1회만 처리:

```bash
paper-organizer --watch "C:/Users/USER/Downloads" --output "D:/Papers" --once
```

명시적으로 `watch` 서브명령을 써도 됩니다.

```bash
paper-organizer watch --watch "C:/Users/USER/Downloads" --output "D:/Papers"
```


## GUI 실행

간단한 데스크톱 화면으로 감시 시작/중지, 1회 처리, 재인덱싱, 검색을 할 수 있습니다.

```bash
paper-organizer-gui
```

GUI에서 할 수 있는 일:
- 감시 폴더 / 출력 폴더 선택
- 1회 처리
- 자동 감시 시작 / 중지
- 재인덱싱
- 키워드 / 저자 / 연도 / 분야 / 저널 검색
- 검색 결과 더블클릭으로 파일 열기

## 검색

본문/제목/저자/저널/DOI 일부를 포함한 키워드 검색:

```bash
paper-organizer search transformer --output "D:/Papers"
```

저자 검색:

```bash
paper-organizer search --author Kim --output "D:/Papers"
```

연도 + 분야 검색:

```bash
paper-organizer search --year 2024 --field AI --output "D:/Papers"
```

저널/학회 검색:

```bash
paper-organizer search --venue Nature --output "D:/Papers"
```

조합 검색:

```bash
paper-organizer search transformer --author Kim --year 2024 --field AI --output "D:/Papers"
```

## 재인덱싱

기존에 이미 정리된 라이브러리를 다시 검색 인덱스에 넣을 수 있습니다.

```bash
paper-organizer reindex --output "D:/Papers"
```

## 결과 구조

```text
D:/Papers/
├─ AI/
│  └─ 2024/
├─ BIO/
├─ MED/
├─ CS/
├─ ETC/
├─ DUPLICATE/
├─ REVIEW/
└─ LOG/
   ├─ paper_organizer_log.csv
   ├─ doi_index.json
   └─ paper_index.sqlite3
```

## 파일명 형식

```text
[분야코드]_[첫저자]_[연도]_[저널/학회]_[짧은제목].pdf
```

예:

```text
AI_Vaswani_2017_NIPS_AttentionIsAllYouNeed.pdf
```

## 검색 인덱스에 들어가는 항목

- DOI
- 제목
- 첫 저자
- 연도
- 저널/학회
- 분야 코드
- 초록 일부
- PDF 앞부분 텍스트 일부

## 의존성

- PyMuPDF
- requests
- watchdog
- sqlite3 (Python 표준 라이브러리)

## 비고

- DOI가 있으면 Crossref 조회를 우선 사용합니다.
- DOI가 없거나 조회 실패 시 PDF 텍스트와 내장 메타데이터로 보조 추론합니다.
- 필수 메타데이터가 부족하면 `REVIEW` 폴더로 이동합니다.
- 동일 DOI는 `DUPLICATE` 폴더로 이동합니다.
- 검색은 SQLite 인덱스를 사용하므로 분류 후 바로 검색 가능합니다.


## Windows EXE 빌드

Windows에서 Python 설치 후 아래로 EXE를 만들 수 있습니다.

```bash
pip install -r requirements-exe.txt
pyinstaller AcademicPaperOrganizer.spec
```

또는 배치 파일로:

```bat
build_windows_exe.bat
```

빌드 결과:

```text
dist/AcademicPaperOrganizer/AcademicPaperOrganizer.exe
```
