# Binance ma'lumot yig'uvchi

GitHub Actions har soatda Binance spot bozoridagi barcha faol USDT juftliklarining
soatlik shamlarini `data/` papkasiga yozadi. Bu ma'lumotlar Impuls jurnali
va savdo gipotezalarini sinash (backtest) uchun ishlatiladi.

- `collector.py` — ma'lumot oluvchi dastur
- `.github/workflows/collect.yml` — har soatlik jadval
- `data/YYYY-MM/YYYY-MM-DD.csv` — kunlik fayllar (vaqt UTC bo'yicha, Toshkent = UTC+5)

Manba: data-api.binance.vision — ochiq bozor ma'lumotlari, API kalit kerak emas.
