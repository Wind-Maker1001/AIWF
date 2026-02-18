# AIWF 桌面版（离线可用）

当前桌面版已内置本地 GUI 和本地离线清洗引擎，默认无需联网即可使用。

## 1. 功能模式

- `离线本地模式（推荐）`
  仅依赖桌面应用自身，直接读取本地数据并生成 `xlsx/docx/pptx`。
- `后端模式`
  可选连接你自己的 AIWF Base API（`/api/v1/integrations/dify/run_cleaning`）。

## 2. 离线模式输入支持

- 支持：`csv`、`xlsx`、`txt`、`docx`、`pdf`、图片（`png/jpg/jpeg/bmp/webp`）
- 产出：`fin.xlsx`、`audit.docx`、`deck.pptx`
图片在纯离线模式下默认启用 OCR（自动探测本机 Tesseract）；若未检测到 Tesseract，则会自动降级为文件信息入库并给出告警提示。  
扫描版 PDF 会优先尝试文本层抽取；当文本层过弱时，若本机存在 `pdftoppm` + `tesseract`，会自动转页图并执行 OCR。

成品风格（GUI 可选）：
- `assignment`（作业风）
- `debate_plus`（辩论增强）
- `academic`
- `professional`
- `business`
- `debate`（经典）

成品质量模式：
- `high`（默认，推荐）
- `standard`（更快）

## 3. 开发运行

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_desktop.ps1
```

脚本会执行：
1. `npm install`
2. `npm run smoke`（验证离线引擎可生成产物）
3. `npm run dev`

## 4. 打包 Windows 可执行文件

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_desktop.ps1 -BuildWin -BuildInstaller
```

输出目录：
- `apps/dify-desktop/dist`
- 便携版：`AIWF Dify Desktop <version>.exe`
- 安装版：`AIWF Dify Desktop Setup <version>.exe`

## 5. 离线机器使用建议

- 优先使用安装版 `AIWF Dify Desktop Setup <version>.exe`（可选安装路径）
- 便携版 `AIWF Dify Desktop <version>.exe` 可直接拷走运行
- 首次启动默认就是离线本地模式
- 生成文件默认写到：`文档\AIWF-Offline\<job_id>\artifacts`
