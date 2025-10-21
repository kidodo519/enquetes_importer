# Enquêtes Importer

このリポジトリには、Google スプレッドシートに蓄積されたアンケート回答を施設ごとのデータベースへ取り込むためのスクリプトが含まれています。

## 必要なファイル

- `client_secret.json` – Google サービスアカウントの認証情報。
- `config.yaml` – インポート対象の法人・施設・マッピング定義などの設定ファイル。

各ファイルはリポジトリ直下に配置します。

## 設定ファイルの構成

`config.yaml` の構造は以下の通りです。実際の記述例は [`config.example.yaml`](./config.example.yaml) を参照してください。

```yaml
google:
  worksheet: "フォームの回答 1"   # ワークシート名を省略した場合の既定値

corporations:
  <corporation-key>:
    db:
      host: ...
      port: 5432
      user: ...
      password: ...
      database: ...
    mappings:
      default:                 # 法人で共有するマッピング定義
        string:
          <column-key>: <header name>
        text: {}
        integer: {}
        date: {}
        datetime: {}
    facilities:
      <facility-key>:
        facility_code: 1        # データベースに登録済みの施設コード
        spreadsheet:
          id: <spreadsheet id>
          worksheet: <optional worksheet name>
        mappings:               # 任意。施設固有のマッピングカタログ
          default:
            ...
        mapping: <mapping key>  # 任意。省略または空欄なら施設→法人→全体の順で default を利用
```

### マッピングの優先順位

1. 施設設定の `mapping` キーで指定したカタログ（空欄の場合は次の優先順位へ）。
2. 施設の `mappings` 内にある `default`。施設側で `default` を定義していない場合は法人の `default` を利用します。
3. 法人の `mapping` キー、または `mappings` 内の `default`。
4. ルートに定義した `mappings`（必要に応じてグローバルで共有する場合）。

### ワークシートの選択

- 施設設定の `spreadsheet.worksheet` に名前を指定すると、そのワークシートを読み込みます。
- `spreadsheet.worksheet` が空欄または未指定の場合は、`config.yaml` の `google.worksheet` で定義した既定値を使用します。
- いずれも指定されていない場合は、スプレッドシートの先頭ワークシートが使用されます。

## 実行方法

1. 仮想環境などで依存関係（`gspread`, `psycopg2`, `oauth2client`, `python-dateutil`, `jaconv`, `PyYAML`）をインストールします。
2. `config.yaml` と `client_secret.json` を配置します。
3. 以下のコマンドでインポートを実行します。

```bash
python google_spreadsheet.py \
  --corporation kinokuniya \
  --facility kinokuniya \
  --table enquetes
```

- `--corporation`、`--facility` は複数指定（`--facility corp.facility` 形式を含む）できます。省略すると全施設が対象です。
- `--table` を指定しない場合は `enquetes` テーブルへ書き込みます。

## トラブルシューティング

- **ヘッダーが見つからないエラーが出る場合**: `config.yaml` のマッピングで指定したヘッダー名がワークシートと一致しているか確認してください。全角・半角・スペースの差異は正規化されません。
- **シフト JIS で保存できない文字**: 自由記述欄などに含まれる一部の文字は `?` に置き換えられます。置換文字は `google_spreadsheet.py` の `replace_invalid_shiftjis_chars` 関数で変更できます。

## その他

- 施設ごとのマッピング変更は `config.yaml` の `facilities.<facility-key>.mappings` に定義し、`mapping` キーで利用するカタログを選択します。
- 既定値の取り扱いは施設 → 法人 → 全体の順にフォールバックするため、必要最小限の設定で運用できます。
