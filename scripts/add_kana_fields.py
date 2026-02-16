name: Fix master + rebuild site data

on:
  workflow_dispatch:
    inputs:
      ward_filter:
        description: "対象区（例：港北区）。空欄なら全件"
        required: false
        default: "港北区"
      max_updates:
        description: "この実行で更新する最大件数（コスト制御）"
        required: true
        default: "80"
      only_bad_rows:
        description: "怪しい行だけ更新する（1推奨）"
        required: true
        default: "1"
      strict_address_check:
        description: "横浜市＋区を含む住所だけ採用（1推奨）"
        required: true
        default: "1"
      sleep_sec:
        description: "API連打防止のスリープ（秒）"
        required: true
        default: "0.15"
      overwrite_phone:
        description: "phoneを上書き（0:空欄のみ / 1:上書き）"
        required: true
        default: "0"
      overwrite_website:
        description: "websiteを上書き（0:空欄のみ / 1:上書き）"
        required: true
        default: "0"
      overwrite_map_url:
        description: "map_urlを上書き（0:空欄のみ / 1:上書き）"
        required: true
        default: "0"
      overwrite_station_walk:
        description: "最寄り駅/徒歩分を上書き（0:空欄のみ / 1:上書き）"
        required: true
        default: "1"

permissions:
  contents: write

concurrency:
  group: fix-master
  cancel-in-progress: false

jobs:
  fix-master:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout (full history)
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Sync with remote (rebase before edits)
        shell: bash
        run: |
          set -euxo pipefail
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git fetch origin main
          git rebase origin/main

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install deps
        shell: bash
        run: |
          set -euxo pipefail
          python -m pip install --upgrade pip
          pip install requests beautifulsoup4

      - name: Ensure scripts exist
        shell: bash
        run: |
          set -euxo pipefail
          test -f scripts/fix_master_with_google_places.py
          test -f scripts/update_from_yokohama.py
          python -V

      - name: Fix master_facilities.csv (Google)
        shell: bash
        env:
          GOOGLE_MAPS_API_KEY: ${{ secrets.GOOGLE_MAPS_API_KEY }}
          WARD_FILTER: ${{ inputs.ward_filter }}
          MAX_UPDATES: ${{ inputs.max_updates }}
          ONLY_BAD_ROWS: ${{ inputs.only_bad_rows }}
          STRICT_ADDRESS_CHECK: ${{ inputs.strict_address_check }}
          GOOGLE_API_SLEEP_SEC: ${{ inputs.sleep_sec }}
          OVERWRITE_PHONE: ${{ inputs.overwrite_phone }}
          OVERWRITE_WEBSITE: ${{ inputs.overwrite_website }}
          OVERWRITE_MAP_URL: ${{ inputs.overwrite_map_url }}
          FILL_NEAREST_STATION: "1"
          OVERWRITE_NEAREST_STATION: ${{ inputs.overwrite_station_walk }}
          OVERWRITE_WALK_MINUTES: ${{ inputs.overwrite_station_walk }}
        run: |
          set -euxo pipefail
          python -u scripts/fix_master_with_google_places.py
          ls -la data || true

      - name: Rebuild latest month JSON (apply master to site data)
        shell: bash
        env:
          WARD_FILTER: ${{ inputs.ward_filter }}
        run: |
          set -euxo pipefail
          python -u scripts/update_from_yokohama.py
          ls -la data || true

      - name: Commit & push changes
        shell: bash
        run: |
          set -euxo pipefail

          if [ -z "$(git status --porcelain)" ]; then
            echo "No changes."
            exit 0
          fi

          git add data || true
          git commit -m "Fix master_facilities + rebuild site data" || true

          # 直前に他のpushがあっても落ちにくくする
          git fetch origin main
          git rebase origin/main
          git push origin main
