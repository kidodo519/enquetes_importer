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
  - `slack_error_notification`
  - `corporations.<corp>.db`
  - `corporations.<corp>.facilities`
- 各 `facilities.<facility>` では次のキーのみを利用:
    - `facility_code`
    - `delete`
    - `before_insert_sql`
    - `spreadsheet.id`
    - `spreadsheet.worksheet`
    - `header_row`（任意。ヘッダー行番号。未指定/空白時は `1`）
    - `mapping_files`

`spreadsheet` は以下の形式で定義します。

```yaml
spreadsheet:
  id: [スプレッドシートのID]
  worksheet: [スプレッドシートのシート名]
header_row: 2
```

`header_row` は 1 始まりの行番号です。例えばフォームの質問文が2行目にある場合は `2` を指定します。
未指定または空白の場合は従来どおり1行目をヘッダーとして扱います。

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

### エラー時の Slack 通知

施設単位の処理でエラーが発生した際に、Slack Incoming Webhook へ通知できます。
通知先は施設ごとではなく、`config_db.yaml` の共通設定で1つだけ管理します。

```yaml
slack_error_notification:
  enabled: true
  webhook_url: https://hooks.slack.com/services/XXX/YYY/ZZZ
```

- `enabled`: `true` のとき通知を送信します（`false` で無効化）。
- `webhook_url`: 通知先Webhook URL。`enabled: true` で未設定の場合は通知せず警告ログを出力します。
- Webhook送信時に `503 Service Unavailable` が返った場合は、最大3回まで再試行します。

エラー発生時は、対象の施設をスキップして次の施設の処理を継続します。

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
  - `facility_settings.provider`: 施設設定（worksheet/table など）を補完する関数名

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
  hachinobo:
    facilities:
      hachinobo:
        facility_processors:
          facility_settings:
            provider: hachinobo_enquetes_settings
  goshobo:
    facilities:
      goshobo:
        facility_processors:
          value_conversions:
            provider: goshobo_room_number_conversions
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

### hachinobo の worksheet/table 補完

`hachinobo.hachinobo` / `hachinobo.hachinobo_text` は内部的に
`facility_settings.provider` を通じて worksheet/table を補完します。

## 注意事項

- Google Service Account の認証JSONを `client_secret.json` として配置してください。
- 取り込み先DBへの接続情報は秘匿情報です。実運用では安全な方法で管理してください。

## トラブルシューティング

### `gspread.exceptions.APIError: [503]: The service is currently unavailable`

- 発生箇所が `open_worksheet` / `client.open_by_key(...)` の場合、Google Sheets API 側の一時的な障害、
  もしくはネットワーク経路の瞬断で失敗している可能性が高いです。
- 現在の実装では、gspread のリトライは **429（Quota exceeded）と 503（Service Unavailable）**
  を対象にしており、指数バックオフで再試行します。
- 施設単位で例外を捕捉しているため、該当施設は失敗ログを出してスキップされ、次の施設の処理は継続します。

### `Missing required header(s) in worksheet: ...`

- 取り込み時に必要ヘッダーは mapping から自動算出されます。シート1行目の列名と mapping の値が一致しないと、
  `Missing required header(s)` として施設がスキップされます。
- よくある原因:
  - フォーム項目名の変更（文言差分、句読点/全角半角差分を含む）
  - 参照している worksheet が想定と異なる
  - mapping ファイル側が最新フォーム仕様に追随できていない

### `enquete_key generation failed: ...`

- `room_number/room_code` や `start_date` が不正で `enquete_key` が生成できない行でも、現在はスキップせず
  `enquete_key` を空文字で取り込みます（警告ログは出力されます）。
