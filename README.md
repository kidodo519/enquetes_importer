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

### 推奨: 分割設定

- `config.yaml`
  - `db_config`: DB情報・施設設定ファイルのパス
  - `mapping_config`: mapping定義ファイルのパス
- `config_db.yaml`
  - `google`
  - `corporations.<corp>.db`
  - `corporations.<corp>.facilities`
- `config_mapping.yaml`
  - `mappings`
  - `corporations.<corp>.mappings`

`config_loader.py` が両ファイルをマージして実行時設定を構築します。

### 互換: 単一設定

従来どおり、`config.yaml` 1ファイルにすべて記載する形式も利用できます。

## 実行方法

```bash
python enquetes_importer.py --config config.yaml
```

主なオプション:

- `-c, --corporation`: 取り込み対象法人を指定（複数指定可）
- `-f, --facility`: 取り込み対象施設を指定（`facility` または `corporation.facility`）
- `--table`: 取り込み先テーブル名（デフォルト: `enquetes`）

例:

```bash
python enquetes_importer.py --config config.yaml -c a_and_c -f sankoh
```

## 施設固有プロセッサ

`config_db.yaml` の各施設設定で、必要な関数を指定できます。

- `facility_code_processor`: 行データから `facility_code` を動的に解決
- `value_conversion_processor`: 施設固有の値変換テーブルを追加

### sankoh の facility_code 変換

`a_and_c.facilities.sankoh` で `facility_code_processor: sankoh_facility_code` を指定すると、
`facility_code_source`（デフォルト: `宿泊施設`）列から次の変換を行います。

- 夢乃井 → `1`
- 夕やけこやけ → `2`
- 祥吉 → `3`
- 加里屋旅館Q → `4`

### goshobo の部屋番号変換

`goshobo.facilities.goshobo` で `value_conversion_processor: goshobo_room_number` を指定すると、
`room_number` に対して `1号棟` → `1` のような変換を適用します。

## 注意事項

- Google Service Account の認証JSONを `client_secret.json` として配置してください。
- 取り込み先DBへの接続情報は秘匿情報です。実運用では安全な方法で管理してください。
