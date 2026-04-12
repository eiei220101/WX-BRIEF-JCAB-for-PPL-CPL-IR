@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo サーバーを起動しています...
echo 実行するスクリプト: "%~dp0app.py"
py -3 "%~dp0app.py" 2>nul
if errorlevel 1 python "%~dp0app.py"
if errorlevel 1 (
  echo Python が見つかりません。Microsoft Store 版または python.org から Python を入れてください。
  pause
)
