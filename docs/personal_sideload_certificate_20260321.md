# Personal Sideload Certificate

This repository defaults to personal/friend sideload distribution.

That means WinUI MSIX releases should normally reuse one stable personal sideload certificate instead of generating a brand new signing identity for every build.

## Scripts

Ensure or rotate the reusable sideload certificate:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ensure_personal_sideload_certificate.ps1
```

Check expiry state:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_personal_sideload_certificate.ps1
```

Block a release when the certificate is too close to expiry:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_personal_sideload_certificate.ps1 -WarnWhenExpiresInDays 30 -FailWhenExpiresInDays 14
```

## Default Storage

- metadata: `%LOCALAPPDATA%\AIWF\personal_sideload_certificate\aiwf_personal_sideload.metadata.json`
- certificate: `%LOCALAPPDATA%\AIWF\personal_sideload_certificate\aiwf_personal_sideload.cer`
- pfx backup: `%LOCALAPPDATA%\AIWF\personal_sideload_certificate\aiwf_personal_sideload.pfx`
- password file: `%LOCALAPPDATA%\AIWF\personal_sideload_certificate\aiwf_personal_sideload.password.txt`

## Rotation Rules

- if the certificate is missing, it is created
- if the certificate is expired, it is rotated
- if the certificate expires within the configured warning window, it is rotated on ensure
- release entrypoints may block when the certificate enters the configured release block window

## Practical Guidance

- keep the exported `.pfx` and password file somewhere safe if you want continuity across machines
- send friends the `.cer` through the bundle/MSIX install path; they do not need the private key
- use `ManagedTrusted` only if you later move to a real trusted signing setup
