# Salesforce Exporter

Salesforce から複数のオブジェクトを取得し、CSV 形式に変換して Amazon S3 へアップロードするバッチです。設定ファイルで SOQL・増分抽出条件・S3 などを管理します。

## 変更点の概要

リポジトリに含まれる主なコンポーネントと役割は次の通りです。どのファイルが何を担っているかを把握しやすいように、構成を一覧化しました。

| ファイル / ディレクトリ | 役割 |
| --- | --- |
| `main.py` | 設定ファイルを読み込み、エクスポート処理を起動するエントリーポイントです。 |
| `salesforce_exporter/config.py` | YAML 設定をデータクラスに読み込み、増分条件の計算や検証を行います。 |
| `salesforce_exporter/exporter.py` | Salesforce から SOQL を実行し、CSV 出力・S3 アップロードをまとめて処理します。 |
| `salesforce_exporter/s3_uploader.py` | S3 へのアップロードや成功後のアーカイブ処理を担います。 |
| `config.yaml.example` | 実際に編集する `config.yaml` のサンプルです。 |
| `requirements.txt` | 必要な Python ライブラリをまとめています。 |


## 必要環境

- Python 3.10 以降
- `pip install -r requirements.txt`

## 使い方

1. `config.yaml.example` をコピーして `config.yaml` を作成します。
2. S3・Salesforce の認証情報、出力先ディレクトリ、SOQL を設定します。
3. 実行します。

```bash
python main.py --config config.yaml
```

`--verbose` を付与するとデバッグログを出力します。

## 設定

設定ファイルは YAML 形式です。主要な項目は以下の通りです。

- `s3_info`
  - `bucket_name`、`access_key_id`、`secret_access_key` はアップロード先の S3 情報です。
  - `file_name` は S3 オブジェクトキーのプレフィックスです。CSV ファイル名が連結されます。
- `csv`
  - `output_directory` は CSV を一時的に保存するローカルディレクトリです。
  - `archive_directory` を指定するとアップロード成功後にファイルを移動します。
  - `encoding` を指定すると CSV の文字コードを変更できます。既定値は `utf-8` で、`shift_jis` を指定すると SJIS で書き出します。
- `salesforce` は接続情報です。`domain` に `test` を指定すると Sandbox に接続します。`security_token` を空文字もしくは省略
  すると、IP 制限でトークン不要な環境としてログインします。
- `timezone` はファイル名や日付条件を計算する際のタイムゾーンです。
- `incremental`
  - `field` は増分取得の基準となる最終更新日などの列名です。
  - `where_template` は WHERE 句のテンプレートで、`{field}`、`{start_iso}`、`{end_iso}` などを利用できます。
  - `window_days` は取得期間の長さ、`end_offset_days` は「現在時刻から何日前まで」を表します。既定では「昨日の同時刻までの 24 時間分」を抽出します。
- `queries` 配列
  - `name` はクエリの識別子です。
  - `soql` は WHERE 句を除いた SOQL を記載します。テンプレートで生成した WHERE 句が自動的に付与されます。手動で `where` を指定するとその条件を使用します。
  - `output_file` を指定すると CSV ファイル名に利用されます。
  - `write_output` を `false` にすると SOQL は実行しますが CSV を生成せず、後続クエリや結合用に結果のみをキャッシュします。
  - `incremental` をクエリ単位で指定すると、増分取得の設定を上書きまたは無効化できます。`false` を指定すると常に全件出力、マップ形式で `field` や `window_days` を設定するとその値を使用します。
  - `relationship_filters` を指定すると、先に実行したクエリの結果から ID を収集して `IN` 条件を自動生成できます。`source_query`（参照元クエリ名）、`source_field`（参照元の列名）、`target_field`（対象クエリでフィルタする列名）を設定すると、取得した ID を `target_field IN (...)` 形式で追加します。ID が多い場合に備えて `chunk_size`（既定値 200）で分割し、複数回に分けて SOQL を実行します。

- `combined_outputs` 配列
  - `name` は結合結果の識別子、`base_query` は結合の起点となるクエリ名です。
  - `joins` で複数の結合定義を並べると、順番に `pandas.merge` を実行して列を取り込みます。`left_on`／`right_on` で結合キー（単一または配列）を指定し、`suffixes` で重複カラム名に付くサフィックスを制御できます（省略時は `("", "_<source_query>")`）。
  - `output_file` を指定すると生成される CSV のファイル名になります。省略時は `name` が使用されます。
  - サンプル設定では `Reservations_*` と `Sales_*` の元データに関連オブジェクト（`Contact`、`Plan`、`AccountAcount`、`AccountMaster`）
    を順番に結合し、最終的な CSV を 7 ファイルにまとめています。


## ファイル出力と S3 アップロード

各 SOQL の結果を CSV に出力し、`s3_info.file_name` のプレフィックスと組み合わせて S3 にアップロードします。アップロード成功後に `archive_directory` が設定されている場合はそのディレクトリへファイルを移動します。

## テスト

実際の Salesforce・S3 へは接続せず、設定ファイルの検証とコード整形のみを実施しています。
