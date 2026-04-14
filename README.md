# Enquetes Importer

Googleスプレッドシートのアンケート回答を読み取り、施設ごとに整形して PostgreSQL へ取り込むバッチです。

## 概要

このリポジトリでは、次の責務ごとにファイルを分割しています。

- `enquetes_importer.py`  
  エントリーポイント。設定読込、対象法人/施設の選択、Spreadsheet取得、DB接続、各処理のオーケストレーションを担当。
- `converters.py`  
  セル値正規化、日付パース、数値評価（`comprehensive_evaluation`）などの型変換処理。
- `common_processors.py`  
  mapping解決、値変換、レコード組み立て、`enquete_key` 生成など施設共通のデータ加工処理。
- `facility_processors.py`  
  施設固有ロジック。`facility_code` 変換や施設別 `value_conversions` を関数として実装。
- `db_importer.py`  
  `before_insert_sql` 実行、既存データ削除、`INSERT` 実行などDB書き込み処理。
- `config_loader.py`  
  設定ファイル読込。単一設定ファイルと、DB設定/マッピング設定の分割構成の両方をサポート。

## 必要環境

- Python 3.10 以上
- `requirements.txt` 相当の依存パッケージ（少なくとも下記）
  - `gspread`
  - `oauth2client`
  - `psycopg2`
  - `PyYAML`
  - `python-dateutil`
  - `jaconv`

## 設定ファイル

### 推奨: 単一設定（`config_db.yaml`）

- `config_db.yaml`
  - `google`
  - `corporations.<corp>.db`
  - `corporations.<corp>.facilities`
  - 各 `facilities.<facility>` では次のキーのみを利用:
    - `facility_code`
    - `delete`
    - `before_insert_sql`
    - `spreadsheet_id`
    - `mapping_files`

### 施設ごとに mapping ファイルを分離する方法

`config_db.yaml` の各施設に `mapping_file`（文字列）または `mapping_files`（配列）を指定できます。
指定した YAML から `mappings` を読み込み、その施設の `mappings` として利用します。
`mapping_files` にパス区切りを含まない値を指定した場合は、`facility_mappings/<値>.yaml`（または `.yml`）として解決します。

```yaml
corporations:
  a_and_c:
    facilities:
      sankoh:
        mapping_files:
          - sankoh_japanese
        mapping: sankoh_japanese
```

`mapping_files` でフォルダを指定した場合は、その配下の `*.yaml` / `*.yml` を再帰的に読み込みます。
単一 mapping の場合は自動でその mapping を使用し、`default` がある場合は `default` を利用します。

`language_column` を使う施設では、`language_mappings` が未指定でも、`mapping_files` で読み込んだ
`*_japanese` / `*_english`（または名前に `japanese` / `english` を含む）mapping を自動で言語判定に利用できます。

### 互換: 分割設定

`db_config` / `mapping_config` を使う従来の分割設定も、引き続き利用できます。

## 実行方法

```bash
python enquetes_importer.py --config config_db.yaml
```

主なオプション:

- `-c, --corporation`: 取り込み対象法人を指定（複数指定可）
- `-f, --facility`: 取り込み対象施設を指定（`facility` または `corporation.facility`）
- `--table`: 取り込み先テーブル名（デフォルト: `enquetes`）

例:

```bash
python enquetes_importer.py --config config_db.yaml -c a_and_c -f sankoh
```

## 施設固有プロセッサ

施設固有ロジックは `facility_processors.py` 側の override/processor で吸収します。
`config_db.yaml` には施設固有パラメータを直接書かず、共通キーのみを記載してください。

- `facility_processors`: 施設プロセッサ定義（ネスト構造）
  - `facility_code.resolver`: 行データから `facility_code` を動的に解決する関数名
  - `facility_code.required_headers`: 追加必須ヘッダーを返す関数名
  - `value_conversions.provider`: 施設固有の値変換テーブルを返す関数名

例:

```yaml
corporations:
  sankoh:
    facilities:
      sankoh:
        facility_processors:
          facility_code:
            resolver: resolve_sankoh_facility_code
            required_headers: get_sankoh_required_headers
```

後方互換として `facility_code_processor` / `value_conversion_processor` も引き続き利用できます。

### sankoh の facility_code 変換

`sankoh.sankoh` は内部的に `sankoh_facility_code` を適用し、
`facility_code_source`（デフォルト: `宿泊施設`）列から次の変換を行います。

- 夢乃井 → `1`
- 夕やけこやけ → `2`
- 祥吉 → `3`
- 加里屋旅館Q → `4`

### goshobo の部屋番号変換

`goshobo.goshobo` は内部的に `goshobo_room_number` を適用し、
`room_number` に対して `1号棟` → `1` のような変換を適用します。

## 注意事項

- Google Service Account の認証JSONを `client_secret.json` として配置してください。
- 取り込み先DBへの接続情報は秘匿情報です。実運用では安全な方法で管理してください。
