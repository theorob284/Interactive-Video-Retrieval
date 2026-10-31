#!/usr/bin/env python3
"""
student_download.py — fetch the Assignment 2 corpus from the DCU Onedata share.

Self-contained: stdlib only (plus `curl` or `wget` for the transfers — one of
them is present on essentially every Linux/macOS box). No pip install, no
account, no Onedata client.

  Interactive:   python3 student_download.py
  Scripted:      python3 student_download.py --packages metadata,msb,thumbnails,videos_480p
  Whole corpus:  python3 student_download.py --all
  Video subsets: python3 student_download.py --videos 31-40,55
                 python3 student_download.py --videos 100-199 --packages videos_480p
  Verify only:   python3 student_download.py --verify-only
  Resume:        just re-run the same command — partial files are continued.

Features
  * Package picker with sizes and a disk-space check.
  * Per-video batches: `--videos 31-40,55` limits the download to whole
    videos by their id — each video's files across media, keyframes,
    thumbnails and `info/`, plus `videos_metadata.*` so ids stay mappable —
    for processing the corpus in chunks without fetching all ~315 GiB.
    Ranges are inclusive and may be reversed (`40-31` == `31-40`).
  * Resumable downloads via HTTP Range (partial `.part` files are continued).
  * Per-file verification: size always, sha256 when the manifest carries it.
  * Grandfather resume: a fully-downloaded, verified file is never re-fetched.
    Re-running is cheap and safe.
  * Retries with exponential backoff; bad files are re-fetched, not trusted.
  * Parallel downloads (`--jobs`), progress bars, clean Ctrl-C behaviour.

Layout note: the share mirrors the sample folder, so a package's relative paths
are identical locally and remotely. SHARE_URL below already points at the
course share; reconfigure via the ASSIGN2_* environment variables
(share URL/token, destination, share subpath).

Exit codes: 0 ok · 1 usage/config · 2 some files failed · 3 verification failed.
"""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urljoin, urlparse

# --------------------------------------------------------------------------
# CONFIG — set these to the real DCU Onedata share before distributing.
# --------------------------------------------------------------------------

# Public share URL, e.g. "https://onezone.computing.dcu.ie/share/<token>".
# If the token is not embedded in the URL, put the bare zone URL here and set
# SHARE_TOKEN instead.
SHARE_URL = os.environ.get(
    "ASSIGN2_SHARE_URL",
    "https://onezone.computing.dcu.ie/share/c1855811c106218c19594d8c49f31edfcha355")

# Optional: explicit share token (the part after /share/). Overrides SHARE_URL's.
SHARE_TOKEN = os.environ.get("ASSIGN2_SHARE_TOKEN", "")

# Where downloads land. Default: ./assignment2_data, relative to the working
# directory (matching the handout).
DEFAULT_DEST = os.environ.get("ASSIGN2_DEST", "assignment2_data")

# Path prefix inside the share that contains the assets.
SHARE_SUBPATH = os.environ.get("ASSIGN2_SUBPATH", "Search Technologies Assignment 2")

USER_AGENT = "assign2-downloader/1.0"

# --------------------------------------------------------------------------
# HTTP backend: prefer `requests`, else curl, else wget. One is enough.
# --------------------------------------------------------------------------

class Backend:
    name = "?"

    def head_size(self, url: str, timeout: int = 30) -> Optional[int]:
        raise NotImplementedError

    def get_stream(self, url: str, offset: int, dest: Path, timeout: int,
                   on_progress) -> tuple[int, Optional[str]]:
        """Append bytes from `offset` to `dest`. Return (httpid, err)."""
        raise NotImplementedError


class CurlBackend(Backend):
    name = "curl"

    def __init__(self, curl: str):
        self.curl = curl

    def _run(self, args, **kw):
        # -L off: Range must survive; we already resolved the redirect.
        return subprocess.run([self.curl, "-sS", "-A", USER_AGENT, *args], **kw)

    def head_size(self, url, timeout=30):
        r = self._run(["-I", "--max-time", str(timeout), url],
                      capture_output=True, text=True)
        if r.returncode != 0:
            return None
        for line in reversed(r.stdout.splitlines()):
            if line.lower().startswith(("content-length:", "x-onedata-")):
                pass
            if line.lower().startswith("content-length:"):
                try:
                    return int(line.split(":", 1)[1].strip())
                except ValueError:
                    return None
        return None

    def get_stream(self, url, offset, dest, timeout, on_progress):
        """Continue `dest` from `offset` using curl -C (resume-on-write).

        `-r N-` would overwrite from byte 0; `-C <n>` seeks and appends, giving
        true resume. We also capture the HTTP code for the caller.
        """
        args = ["-o", str(dest), "--write-out", "%{http_code}"]
        if offset:
            args += ["-C", str(offset)]
        args.append(url)
        r = self._run(args, capture_output=True, text=True)
        if r.returncode != 0:
            # exit 33 = "HTTP server doesn't seem to support byte ranges / file
            # already fully retrieved" — treat as ok, size check will confirm.
            if r.returncode == 33:
                return 200, None
            return 0, (r.stderr.strip() or f"curl exit {r.returncode}")
        try:
            icode = int(r.stdout.strip() or "0")
        except ValueError:
            return 0, f"bad curl status {r.stdout!r}"
        if icode not in (200, 206):
            return icode, f"HTTP {icode}"
        return icode, None


class WgetBackend(Backend):
    name = "wget"

    def __init__(self, wget: str):
        self.wget = wget

    def head_size(self, url, timeout=30):
        r = subprocess.run([self.wget, "--spider", "-S", "--timeout", str(timeout),
                            "--user-agent", USER_AGENT, url],
                           capture_output=True, text=True)
        for line in r.stderr.splitlines():
            low = line.lower()
            if "length:" in low and "content-length" in low:
                try:
                    return int(line.split(":", 1)[1].strip().split()[0])
                except (ValueError, IndexError):
                    return None
        return None

    def get_stream(self, url, offset, dest, timeout, on_progress):
        """wget -c on an existing -O target resumes in place."""
        args = [self.wget, "-q", "--user-agent", USER_AGENT, "-O", str(dest)]
        if offset:
            args.append("-c")
        args.append(url)
        r = subprocess.run(args)
        if r.returncode != 0:
            return 0, f"wget exit {r.returncode}"
        return 206 if offset else 200, None


class RequestsBackend(Backend):
    name = "requests"

    def __init__(self):
        import requests  # noqa: F401
        self._r = requests

    def head_size(self, url, timeout=30):
        try:
            r = self._r.head(url, timeout=timeout, allow_redirects=True,
                             headers={"User-Agent": USER_AGENT})
            if r.ok and "content-length" in r.headers:
                return int(r.headers["content-length"])
            r = self._r.get(url, timeout=timeout, stream=True, allow_redirects=True,
                            headers={"User-Agent": USER_AGENT})
            return int(r.headers.get("content-length", 0)) or None
        except Exception:  # noqa: BLE001
            return None

    def get_stream(self, url, offset, dest, timeout, on_progress):
        headers = {"User-Agent": USER_AGENT}
        if offset:
            headers["Range"] = f"bytes={offset}-"
        try:
            with self._r.get(url, stream=True, timeout=timeout,
                             allow_redirects=True, headers=headers) as r:
                if offset and r.status_code == 200:
                    # server ignored Range -> restart from scratch
                    offset = 0
                    dest.unlink(missing_ok=True)
                if r.status_code not in (200, 206):
                    return r.status_code, f"HTTP {r.status_code}"
                mode = "ab" if offset else "wb"
                total = (offset or 0) + int(r.headers.get("content-length", 0) or 0)
                got = offset
                with dest.open(mode) as fh:
                    for chunk in r.iter_content(1 << 20):
                        if not chunk:
                            continue
                        fh.write(chunk)
                        got += len(chunk)
                        on_progress(got, total)
                return r.status_code, None
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # noqa: BLE001
            return 0, f"{type(exc).__name__}: {exc}"


def pick_backend(prefer: Optional[str] = None) -> Backend:
    order = [prefer] if prefer else []
    order += ["curl", "wget", "requests"]
    for name in order:
        if name == "curl" and shutil.which("curl"):
            return CurlBackend(shutil.which("curl"))
        if name == "wget" and shutil.which("wget"):
            return WgetBackend(shutil.which("wget"))
        if name == "requests":
            try:
                return RequestsBackend()
            except ImportError:
                continue
    sys.exit("No transfer backend found. Install `curl` (recommended) "
             "or `pip install requests`.")


# --------------------------------------------------------------------------
# Manifest handling
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# Embedded manifest index.  scripts/build_download_manifest.py patches the
# blob between the markers below at build time (gzip+base85 of a compact
# [[path,size,sha256,package],...] array), so the distributed script carries
# its own index and needs no listing/hash fetch before it can work.
# --------------------------------------------------------------------------

#@@MANIFEST-BEGIN
EMBEDDED_MANIFEST_B85 = (
    "ABzY8000000{@h~S+C{EmEHGMF6xO57s;3h%6^a~Se7ld3{m2XU>Gry>8^pUOtMW0)@RpmMe_d7Wt&06P^*}AEANRD5qnr`#~%L6"
    "H-G%*SN7X)e)r9<zWMyS=3@Hh+i$*n^DFO*$vK)_bNKd~-~YpJKY#P9|MJcE>r0#MOVgjfeERa;;<x_opMUoD`!@dJ?Pou&@#FX3"
    "uOH@`Z~N0vpTGPaA2`?N@zbZ@fBE#|5B$b&KF#&x+xus|{p?@-`ZvG)Uw-}X{{EN$_FumJ<EJmbd;8<3@7It2dcOU4Z@<x>-u~mi"
    "|2Loi?ydbW-@g2Az5Rau^z%QSkNEuKAATC^?b8qY$2Wbz=U=@2`uq0XcmM2d{`ikSeD`D1FMRs^^?SU1`SI=d-?bm!)*shTf8ysq"
    "{Pt~q_j&z-@AvuBe`YH_x{^GHu|Ivc*snFlfBVgc4Sn;gFF*ZZ{XgG+^Zn=k7e~-Oe_21heg55#U*7tUfB0dxpFZ&c|8(E~{JZb|"
    ">EGeIzy0{bdi&3xe*dfUhyU>F-)MtxUpIJ8<NV6!FYTu<zg#*3j^y*(&pz|Dewcqgmi<%Xhrf9H`?b!w|M=6}KmX0I-#(8Yf6_Mf"
    "SM(`A|Bk(X_x3~k<EP&?eW};pZ8kC1`r7+n-}sl`{o(umL;LjI=a2jU56@5Ee)eC}+rRoB|KE4+jQRcl{quihS6{#E*`&VHx9|IZ"
    "ZTlB*|KfM6{eE$0fBf|M5AC}nTJ5*%Y^?wE<3E0W-_?Ko*Kfc5>C^oD+DI~?L|<$)AMerplkM!r&9k4g-sk@#6!ZI!8`8gj{Vv*("
    "w|{j@|MM?gif><jYCmw!{HJf<{`>zyzxeIjMjp<E{o?K4e^3Xc?@yhb{PO$uWgPkYyuN%8KO0i^mRqQQ>#P6yCjMdl>Fce3{NDfT"
    "`w#in*5|+awg0C-eD~eYe_Ee^{O%9>$=m<-_y6kc{lotDt^Awz({FVtf4+@>_V&X|+<y1zx4--O|HWN>`tqmO$2}YUrR4F0w0W79"
    "Qhj#i|Hm~U&A<KhL;LRImjB(KfA+dAoBUth`1{}dxcRg6Tl@SrKYjl4)2IH2FYM=M-~T@R^7|kE_{;CZAOG$bZ~yY$mi+j`cYk{O"
    "w2u)w62kSpVP1YfKi3aG{^7U3`>@00C5SGTl>fhX$QJiEzxq%A=^L4{U-GxS{l)+JnQ#4V4jfRo$<E<=u5<e0{aPzjUuvphWrGwh"
    "Ux!)EhMs&YA-6I5n6cFsO|6b^^;quz{o4<#xc5~YB4=iezM2`WwI#$5U3MlSlcf!p!WyaAUZ-Ef?$M`DE{)}5Tg!K6Yrn3N{=5p?"
    "k8CCsZ&z`ln=(10R(fFF9+K<96j!QsX*pNDH<hiW9=vU>u-~RX-<FB_&&wpKe&wS$hZ1+mEetb98heGdbJ%SdzbeZW>teUwl@-_I"
    "16D~rrqWEiu9D3AD#@{mS!iuD!S~W?k9C{uo!NW@H&ogRWlIj)%G!W4b!5I(4&4qe%Uo9}??>hWM;2?&mETA$`?x}g$u(}AKd$If"
    "+TK=I>|C2ouHAa>dDxN+=499D!fl!Se*eiLq$8zeecZLTz3p;^+jTCC?Sm~nA$?=n&c!2pZWU`-BRKCk=~mnx*Hzx{KN^nAx)N)2"
    "qZ+fv-YchFil3v5Qjxjr_I9Zu@oXr^&|GJ+!Tyl-7_O^W6W>>{oLJj!*`1yDzRfUv7^{XhW^$~CVASDTYMw>Hv?F_yywk~=I%AII"
    "c4F4Pui|6+_M04CZQhKMV;XVH?q`W3nPyN^t}L__i_h$&ag-tZT1LvOhHAy`;&qjGQMTFQh`U)6-C4VDJ3C^V3iiO4<lHiQBVvA!"
    "$zDUr%Z4C%NZTTnBds#*ZJF?HWNKzJOP}M4v%~k%>RMyWu}bB3n~lTrVSBKbu$KNEflLd5nagG_^D4`HpLZjZKvRei_GCsJG?Lr$"
    "Fr{-f9yO|fPrIGzjZ<uUxViZYwfZ`(O`*oo^L3R+FN@*CESAqMS+cNPi@lraS~sb4NSt0M$ir+Po`l^T(WmHhUfahyIybtmtGxDt"
    "C?_<gI)`4TS;h<)nYo*rDCYLP^L1SbhAr~n78+E;MmZ~+G&Le{w^j19io=reK?cKO*GkCea=leQ@>-acG<CmwSH44JOTL?i*i8(`"
    "Dy?INg3Y&8ihay-;QGxl6>+Ui79>pH(V4k0R;Hwgc3XBkrz@uQUdGrRkmcD_o0trn=53kk9~&c2=CF=^*p5KsP&^;R4L6Ly%r?#z"
    ">#DH~+(|_IoJ+m+X%HHOn!Wk!D&g5oW|@U5@S!QrxeGRIhZz#4YrEau;vWk|=Y&kO!EuWt$B4Y)tC81zb!E2JcPT44Om4q%E;zG+"
    "aYEG49DG~o98&47Axpa_9Pn&qchufb{sSUDCwdfb>hH{RWFGE?3Y2L*k|aN7dT88-9}(B+5&~5`-Uaz^8>7mH=Yc2y12!fp)=;ji"
    "yc?My=Ra}mxc*erYH`(Njl-nYk@!dq8Lrdfnj<iP<)-nuhtOIwc$#KxyRKs2r3^@p!Sv{S^4(fLBf^W@<(dNrlu;qve~hgQZpbe0"
    "uwxJSIPPk%C|h$~<^B3g#Q6J(7e`k&r<G)SQ!x=Ow;@hI0OFJb#;fOI7RDOZS1|otk823r-nUiUvx?!wERL#y8LTNQ_r+J`2GS@v"
    "W;vbVvn_ds;xIAzNlXtv%44zbh|q=WDlfR?(M!HiM&nGBEd&j<1l}llXZ#4;96MS!Ar0CWAI5SSH)J_aKpAH6wv0E=#mM+~eh;jP"
    "9me}qwk5a4u}sr(Dif@N_{sEFjO6Z8F4oAl+ue-2d1V*g5k?Rw^Bj5vQ(&_!$Q1B6c0;{4A7yK1R|8X>T?2_?pt`~NH;Z)!<KhIb"
    "7ZcvSvXdq2!OjYN*)fJvu++c@+eYC^MyQqJtQjR;U}d+_gYCG-<}7zIKuh+z%F8Ru`F9VHs3BxCF)9^l=ye&PH1<+<&2xr4ZSy{%"
    "J)!k5=So-^tdfNe-<FB*M^*zGlMrSc{>FOcib}TD#twM_lxCkbw=|Hw)D^qn`}mEi@V%mzewjA%b(P1*RBQ~!z-}CvOiTl2;2rts"
    "0?MD!tsEPGc4W{K`p)->oLO-wZMA%r>aQ1*9)#iX{h@*F@md=P;x<lY)$W4DdH{|v(6)ohZ1w?#2JXeO5sHcVF?kp6?Yc^QFgbcz"
    "y6;%1&D}H$P+siA>R34#0NVh7rV2U}SoaI4Yb?Yga*F6`DpyPPz*rm<I0k72Y-7L@ij5bM&?Y=i$08TZ99G7M$%Z$uykjB|XulVt"
    "(cQkQWqwEgF0z+UGw32@j3fxzVNF0QxD;EB<zt~tNXEj7VfHuXvwE|0b0hLQ7|t7x0hYY)MT9g+JurqDbYY?gg=5;`U)aydqNAeZ"
    "(#2FFASA&?oIJMa11n(K_4dpAy#(3I$!A9)6vquEPE7aFadsBC(a;S{zvJQ;NFYaBoAIM17$3IZx%=&xceV8L{56B}Q25GuK^PDN"
    "tTMg;=K}78qqW6N0+^7s-dbm$b|Y!Cfjz3bu2LUwSvfK6ZNx4H$mEuEa^;|j)dBviWssf~y<7|M%1H{S1A|!>1i8y-H{=IZd^a)Y"
    "$V$bb)C3F!%I6tZjClh5EG`X+z?y=dH$vQiOSy3MYuVU>j9p#xCd&R{F;%WVdTt-R=F;;3n}Z`Z0F4DWK?8%yA$j;UmcI{&{prmD"
    "VdJP}fo|<}m3L7Ft_b+|2{p-!51*K}%AQi;TbB+{MQajfu3~mT(jB0ITSJAT?K?JKTDq;`-u)OZ{o8N0mwg<d?1=~A+#q?uMd)cO"
    "a*mi6$9ZC@d+6K~aG9^;q&2A8FxOSy_u>WobMqFRE)wZ!F}o=^1M-_`Kmw`(tY-t9P6}j{X3^!H!Audhlzh6b@=zsvsB%T$h9JYv"
    "+@}R$bcrr37PkU0wg?=Xox)SoaAF|$4KL*0u&{Gomw89AQSNf#Rsb2LwweO)CHH{@An=}l!j;lGYlvwAkpLVywFY*#9koPWZmT>V"
    "Ce$LVoV`d2UKz&$$`GA2_;!nZh5~~AV0mMMC}UJq317Uh$yiIU$bBzQ)aQtDhhB#!8fGmty37TW#5@+eLc{iSZUZZX9R*!1fIeHr"
    "G+@D-Tl~sxmB){9g}|7&+RSbNVvs3RJax_<n~1&?95I}yWdLOa6IiT*?heR+)MXq(zTy609!3Tj0r<1-!bYZG`Dl<aM~A7wvTz4("
    "e5Hag8Gs=M5BuCZht`+FN?RQ1b(x1=1PK|-c#B;`Eu6?7*gql6eE))aHkb?txfCPo-f(CfB<_HvfSbkNs8V=<f6QEr<OQYgcqI%m"
    "uEPxOeqrWuFK|fMS4KBS$QIdlxq8gOwxB2+wJSH^A09}KLE&PW^9mvGbrbq-@bTVn>lMiKEQT2o0IxV3=m(z$!`P`Un^*QY^6M&("
    "CDXmE9pN!i3|@Yz`D&n+eI^7JW{ejFa@v8<a9Dh-Dxe1pQd=Lx-+_Pd?`jzeBnF*_O!|tu74fpj*5;cJbY{xduWcIG&`bq|<FN9P"
    "PW*7&xXN9~UMLn;g2sSV{Mdrt7H4d+=&+<9aMxWl6T_M1mXR`P3y=_?SWFmb6ztbumwDezwF3Ug)`Ui+uCRFSSOD}Ll!7~qIpFaz"
    "H2|2Tc-=rF{o-;4R|FmT8yEAkF-A7VPQDg~W~4Ah!-p?8N2xEj0E0nSJ(t54(GVB$oYQvCc~0m_zL6ME))T-K>+kR_NLd8Y;cfU-"
    "tPtY01*?S0mRk#k248N`@0~qP3@%f)gXh+KUF98L2H_Y01VkU%1jvJ9`|cJ;HZqqmfoAoDDll3iE-((($Qvmr19kznUe{IL?*+Vv"
    "kkuW)#)1-lVD1A(jXwuWHO4YompQX!<M7k55AZC+9M|gb{d2hMD*0I@V=wE3iCC_4^2LyV4h9c>S33p_+{*Z4G^6N-FkmzWQh^gS"
    "76ga9+*T>?h77h3hZ)`VjC)sr(e#Q13j$~2t4MtT7@6&X#v|-fs%R4)d$R9_MC^K9<?%34;cE*@(jkP+#i!l?D?nn*6|}609VJ=s"
    "!r!T*1>h@)*(+)f{=n+pR;drbQ25%Xrl@<4+W_)X;MOzLxsSy^`9OphGMo6~9A!d43;ZR)q)YQ~+Z*u1e7(z<JpbHg0AQjHDiwae"
    "Vh0n5tl=G@vvIi~kOpqX-l3EDo`f;l6+dp%?e^mX$x=A+%Z=^hPWKKtA0V*|Cm>z#v^kqvDjwMyJqmF`F5tR?8PzXu29*4|O8oeE"
    "5f$Ixt_5LM<jiOv0pP#^VyFZ#cL(Bh0N^NW0SQz@2?*ymRQ<Nf^F)UFA!Km+Jt05_S<Yy;6Ge3#9VQbl42r^|;Rbxg3P5b7u6Cg*"
    "rQ#pzbu-?7490a)mKr*eI;{fy&7ssD<6*I6SkJPmh2^?6(0@}i*N7G6vVgRC+z43={@F}LmR)ktYiwpTkV@DJGL1ikiHTJ<v}P;%"
    "ivz_CVfG?$b40ne2;04~7*ORC!6=GfI)r8Dm*=c{1Zxo_p0}LvE()G-S4bWpYG%GlTwuV3%DQuvw^ic%{TD?^ao<%|T~CVoEty)6"
    "xq_6#)F9!A7O)J^nKnRt=Zu}h$*c~_hGAY;d0yg9Fcz3v;g<Y1wrX$<tz!LgJkVyy9j40{t{zO|6v!AST#A(l;)khT74vz%K>Tqr"
    "m_@VkF<`X-8dkV?ILtktVeo1PWj3QYLI?cA$<Wjc0pOQlYv#Jj`(~<wJ~);HL~3yaJu_m)Ktt@<v;>HmdW``VAp)sJKlRKTVhjzK"
    "inVC|D$9?HIR+Ddv=LNTz9|Z|g3yw_H|{FvZ3(N7Md6e%F5DI;3$5m7NdYJ{^R`NTg-J8|Z$~rLaJ|I=PaJmBZ)h3HZ?azY*h&Sq"
    "fy=%8G@hWAKA<zf?y#>aldONjq}XK<USFVgfpbAaJYWe=i8YOZ>qeFErWs?VhliL&n{j^2SE!od)~>6(Kv~uOhnA5n;0dG_Gf+|S"
    "Qm`i(wj@gfT=pP+zI_wSaT7HiY!Zvz+VzbzJ%%}pD`Tm!D;!VIYeI<pwg>oERzm%ut}$+5Pp-k?TR^igzXITdu7l^(b(IH{B`Idz"
    "c*n5yl<fE5JaVtQpo&<wVM0G-35tf%K^$D=R1wLJv?&^JljZbe6&#4s`3=4pvdKwQh5bv!{^MG_X$}cn9)1Z$f`}(j(3E8d9Rhn%"
    ";&t82V`N;EEt>!#J_;%UHvyM%!2_?G%~<G*L8Q5AkJ$!m$SGqv;T-sc3?@qMy3F&g2r1G_d=I7Jnk))nQ956^T;ZC5-S$AIY%2sG"
    "R0Yz_seoaEGB&_XiqFjJGWiMmX2twD4aGHax&;IbeC&K(B#bW)lk%9bfMZaq&gxkvXA3=pq>z?=RhifGrB=b-11&~XP8KdDf#zY9"
    "d2lFWc3cmbY%2MpDkEsawQ#BU%{G16vESfwdV-5wBg;TxGe{#k_;p8&2K2S<xh>cpG=TOdkbYepm!47DOvV{N6sBwk{@Z1lY|@L9"
    "qnR5i<rl!PTPA+2GP@6Yiccsj$Uxkq4|W1lDp(lsl1KKTSlifnljiK7@S;``)=wy!!|w8H22UAA+|!>3Hmh&HyMj^$JYVY8`vR01"
    "BHhuU>sxGo-bsoq^XLf-)0P;k@OFYga^exvh0f#C=EVGaeR-@mYYZS^UwCi4nm6^j$^-r_`VRmL(_``D0B*ov5L=hP=LsK&T)+Xg"
    "GX~FPY7@l4xbQCkCq1cNpRe=tOcDzx)-0ZU#4((>Iba69gg{|`xdCt=`mrQ};xD6f7Y$N{?6E74poQxy<)KTMe~t;+1vSElHx3zY"
    "&=qvEwS~I$b_DIiS?f8arWvxHeR{_VDNuN&OF6!un9E}R3i0C#%S1&M%DzJ9iXadk%gZh6X%60O^enqe%L8e{3Y5K@7~yS|<X>4O"
    "n15V2h&%^{r;yix!i_jH@=yU0QGz`e#;Yz4EJn~dnuAHlyz$@j&6edSuOR5f7sC#p$7pguxy(F59r0wE6_43+%kfiODmUMumuO=R"
    "xN^)Ii0W@BOnJ<I6fA?SZoPO1y)2q81PP&@JtE0$3XT?Sfg)qzJdkPVjn=xl6n8*FudBRYe^S&xi8dO}9dZKhhMZ*l80!f74SXj)"
    "l(Pb(Pgs+<lXB%kurkeN-c~6OD+4`n|1~xRsepV?pdcH}2N)bR=Y<KY2^kBaIIHZb4St_SS$_q$6g$}9y37MI!b!!zQ;dQ-g2y=U"
    "2`2QQez)M`vY8|47D`4bM=T;3*&6AqFlWAk40!J=uSB`je&HFFnzoQF%s<k;;QjTyy#qD5=~&%@doFRM0MEdZ1EMVFmv1^#9xM|T"
    "VB-7ZG=1lwKrmD95|CTq9fStgulN~1f_AI~t>IfhYWA`TGsO4W>AK1jDwmiUTa@bo*0SJv2v@{@&Sg)aNAaNT&|uN?iTczw3*gH7"
    "ws+D?)-57ip11g~yv*~}Gxh|vXw|tXje*jLSLSK@7`?8DmdF8tKlxi8-%#x+NH+-Ny3D(rIis{PSf!W2M_U%|5yzFd8DZzhvWiz6"
    "o`w)&Xppw)dPUd<+_|AaS8@K}%i>`f;Fcy07;fyf;3+GP$R+2^dI0(2AV?NZgevt2vc=R?;3fd&>dQ*{pfp9H-^~FBC%|+9RERA4"
    "YFEsdZp*nTcG_x%0LWPIPaMV86AgB=T$LujkePPVoni>!R(>fBxBJ8qLNP}IQ8d_>&t7>m$o_CGYo8T2YxU%x?QYj)o-@mOUow-="
    "g)_}ILO?k{pRnV-yVzjr1-jWP<N(Eq1tzT5<~{9zbE<;jwn}-D<m{nLxEWVt0GZ=}^*wQ1Bg<U#|1;PI$?Z;}webiwgyB0dBtz%G"
    "LAO<2%nbeb_8auGL<At>sNzCumoonB7?{SGW}FLe3Uz`7>CrIgWmek4GC@DOEK^@^rB&fSXuRnSV*{f!xE$nqWE@J+YZ<oAic$v1"
    "Xad7RMx<ffSA_teN7n?V`g&g!nr*%)x`2Hwy(tX~x}*dUgt;I^se#FtE(JhfwxWRiM#z9Gz%@4Hy2|_g7svf?yAY|9SshJ?o9oSr"
    "J7P?LXkL5|?-IRDjl)Fb^>Xh_15QJ%%gr=b|MCEe|0Ohh6acZ3f2u&nR_=EImlZ5_7)q2i8^l*P8e>bqKpQ=xPK&F#-b{GKWkqHa"
    "v;wCAJRXX_1@N}x+g1!So(y|~rqsjU9DpfLy*ezVp%geyw%1j{^X{({>5b2XXimTuFoqMdgUv)RO@~eKGu&vI3FI!9gAG~;pAsMN"
    "w$<H)xyJP5d?^=i<9b0RTyJRz)Y@S1sJF1JET#mooDr~8jRhI{JZA$W0uPr$z~*K$>toDvIW80ZPS`4>*(OX`s6Nb-v*5yNsHRu`"
    "y8dGtfX<dNG!w4Qms=pu>ncyCMB?@#U~7Q9ZF3WS#ERiS`R;nm0^sYZeCgGk4@Zh2p9MLL3e)lfWc#*CeGGFxX~kh=yi7vIuw9ND"
    "5>&-MP62d83lmzJOaV8#PbL`Jn&359+%+!#AG`1b%<IXN@E{bp3kcjVpskD@8_7$^EVvYgHMssBK($%qQdy>LvEL0u_t#b4{l8bD"
    "O!LZl1coEDVJr>>lR0x-LYu332XF2sG>#hboHlT@^Dj#|`W#}vt@6sps8QJkI2f2CpADH+Y6rL7=HwiKoQga(r4o18OT<!`DLBjZ"
    "6f~2~rRysBQA<wDxs-EOMy&Fr+<<Ffk!>}c{Rnt0C9Z2#v$7z1R<wDxw4t~YFM3<$!7@R~%Xq!!PL6%YW#gj&0D}_(Lg1}jR+!X^"
    "6yiz~00bu%_>0GHN7t@<d7?ilDz^mRG(mMZ%w-x5h;s=}$zAYOn=(J!+;z4p1`v(T*ca{$Y?$?^eOu*;$_Bk*D+@PrT3{5x7R9YY"
    "8eMs^Sm#|g7-x%-SDZaKw<v{G+Y`BUv)xw7PjW$#?U&KVtjHNcDC*FG7iFusKDu9Ay53Kd{^Y{~Ty2a@S0)k%Tg-Kp$M&BiYq_=N"
    "GxvbS;Kam>1Kxwxn*zh5+tCF5dKDWjUMbEST&0}4Wae#|2Vw*xj@jXTBOtd;(Te8GL>DJ`!v%hVQQ(dvJQI22P`UW5EEz11OQWr;"
    "^4BL*A_>VBrHf7=tcpb%piVBPW2+PZ!amPB0c_$g-Ft^$gZ6+h#M9-oud@6+%Gs!@0?6yc3&S*a7f=>u=Bw(J42lDd#OaUiGe~A+"
    "6biw^jYH1xcj(&fYPji-VrFnqz|;j5@<CAySS-Sfp~6|hql^R0K-ypfs3*uGI2eiEfjacB)axow@K10_6_N|<uTnB4jBao(U=Ozw"
    "5GbemoRtz_Ib*q;pDOo3hm9&^L$CI>iZvgru(142fDYdbD>D#!P=?ZT(ftlgbC@qw3Lrg^j-*H$%oZojRbm_5b(JSaR&aS&Nrdtj"
    "TswK63)nJ2ToWH`l<cc~X0vB1ZB?cHRL(3dWJD3Qa$ChcURmL?&Hct>x}tJQ;dTR+D?>a9)G?ATQd<Prz-+v7OE8}Kq)j3L*V})d"
    "{=CqVlZbkbAJ+`%TXOV@>{a&Tw=%N9V6Cn`aRnH+sA>Y`%{#%}aNWy;$s9+5#nc0dpA`XZ9Y~wN09f}3LULIZGGMW(bUX{uLl%&{"
    "2*rSt@ZH$ab(zP?*aRzcU2mHy!XAcQm`+d<8_eP}H3Bc!O!!zVE1U+dGhzl$+m^KaxP|2e>)y{SDD9^fj;d^>lJ*gltqLCEQWlOB"
    ")*FTRnjT6)h9!WatH%|97L<vUuTmCYuQG~c>O*kMUI7?ou3*prJDif8$!Cl*$5?9sD;c0m_c6Bx$mx1HDbRu2GEe$)(Ays$9H-I-"
    "fd2SsuV;ja>&1KxZdwdL+s%4uFt#dvCxT`M{_lLR+ba1l@+3{}N>zqw$5IQTJWZd)Z?`4@W(^4w>XQ{u18<;lh>f$w7N?imbzS9&"
    "F*<?2q-R8s{L-`RNUBvTxR7x2;ju_tNo6O)AlaqxTs3-Ln;Y2VOT5~$CoTtIK{aQq+`c%UGQXy>dQ(x&2;Le8?_lgnC17BERfR6I"
    "vohrsHpw#0w`D%OF^b9B<+x-;tV=f_Ev)|3GZ}^%cL_=_gG&Hl;6^cPkbZ2aQl1evrKMe#aZlbK2w3DFY6^UH5n!}X62uhMP?ek+"
    "&{Z%E(gtKk&P{?<H5i(no?Ttp&2d}Be#m4%X^<dWKrI$(Is$)42A0Cn>50Svol(UR^qSLg{*xc$s=-jonV8oYgZ)s7qs9>~fRn07"
    "C2+Qwssgg0DqJxL1CriK_l=G3kQy8Y@jE%7s1qJD+>n`jN*56YWwI#8-Uy?Blx%br;aQ<Ug#rFZ1^+3kl#z4dXR9lz5cV0c&p4s0"
    "XLjNBlzFo8i;6E2p8ph;>CGBlg%`mC0MC9yTkr=;J8DHqtbnw}zTgGmn|590iI|p?EN7bnA+R%K4KYxqtk3u!Wy|ot0ehOz9)-L0"
    "{DT;)ma`AV)zYnW*#c4CU9(f|svQu@X1NNRG3iixjEe&M1`KT=&?1wfYyCW(z-d&1Fd7Iqt^3V9?=tv9rI<{92Y^}uHG3*P2D^++"
    "1x00y2;c{9Aj8SA#~Q6NSlLZg9-yl?1K^$<gAh*!`$)ijWLc4npt4jLvu8`_M+AgorBen$dxyXw$}q0Trt#DJZ!jj8U!iP~%+UAd"
    "-W@hXWe<29NK0!5Dr;L&2Ai_(>%w9RHv`nc3MPR7BM|5oFthHlW>5*eF=i!vEuYL5$?(dG-AM`2)nSvlk|0BDy)dHLuEThDnO)M-"
    "VCR=*Ud07K6!8ot&|&Qxpe$wIyWyCesxUb(-&6u%j6QVZp@&s<vs-N;)I%Wfy2`5-OHY}hH;f8Y6B`M_SWWMv3dzl^`q5O<p}0*Y"
    "%(Z){?sC=2Z}L-2>2;YW$H<%l`Bm}xO1o8os!e)xP$pK<jRTewxD7zVsWsOGmZI1hC!gSj^L}LIB>*ZenP79Ux1M<sw2^R>NoC}S"
    "+CY*;0zv8%?ajSbWg}vJc9_`!>TXHwfQ9EVD=A_Q4#vrfn+1H#n!8zr6WBojG<8U8s}S=6I#5sv(r<KV=na}w({+_6k<sHb10Ejt"
    "sKom{oZb{LZL;qQlSKSJJ7gbl4^!9?P;}I$+&LJ-v{!28pBIxPG_w_k$$U{~pxjfL=KK%R%9pMle}$vN(BY0n1r)TmNLq8ky>;K-"
    "vcSyG>yPr~cGJTY7hUvv5S7UY(yI<rpq?3tb@&?-8Eo83gZkhqJ!aY}G4Pg@$ttf*1{IYfNR16yPR+y`^%~m2$Y`gWbHUmN6%lw~"
    "xMHf)HHQh~&$2=Dx|i_mMI=+Y$bC#MvMUN$6hgvDD{fu3=y6uIr{ZOHZlgm2_Zj6iXE@@0m8axO)i_(#gb0|bu1kUXmtaBMxcHFV"
    "3bV`+-NZ!VeY^wEc(8Q$Un{jRLDy9t#1vH}moZOqG^dBlg@2hqzol}YFueFct~*s#L~Z4R3JK_yLS^Br&OL4(^Huc`l*lHzgmo>P"
    "qauK<cvX+}h~glro=;c!2Z#(#MEaCP$%(P|=r>>FYGWQS8&ti>r6@&Gp%MqBfhMnljRaTi#91k;FaZ7CQa+JBxpjDE>kf6*8~$yT"
    "Ct?D~G$;2H;F2sSwXwrM%n3`G9GfaFJ763z4FUuD#fpamPgT7E<Ij^bx26It^I~QaW(*(fZ|ntpfWw9e21K|<L0MD@SdrbblGOvV"
    "!K&g5#jas<ib<ub`oCVx^<Y{mGKGg38Tbtg2RyM9$kRdBx6SwxrRH&%c-00koCvd?Y0$3T>dj;NC&pa7(lV6(uoOSaD<Papf{u!p"
    "@-H|B;IY!O+d65*n7SE<uPT@Z$*rEQ!u)C~5C#81My!E~^Vz@^L}vnI4B!U%$p=N{1gz8b13^XtfC~{P4YQItxCwK3!aw$FTIMK6"
    "v0W526HbLvfWabg%<bT2cWJ8vQ04w@!{4BtE1TG2&GA;fuCVGu0EPbpN~C;Y1gk244KCKyOavs0cjWFXQV<~iE)CE*-^~K$RILU4"
    "!*JY|d0w)jo@FsM$Yj(*Tdk`F4sd%5HZI8`%<5xs;N>ayGKx5sT-93u2eJAs`P&BnAz=<_R?n1%!OW`4P{EAq?>NMuTJnf*;r}&<"
    "NLXcXZXlFi0OYKu7iPOH6J80EURn9zVS~p+{;-P?a~O8sQ|(G1xIPSP^=gSD#t(oDQ+G&SPQ}Q)UN`fQne)hC&9r84Y(wR~!N3Kl"
    "3;>`h>S<;yR0S4`5!=3Sg0l>KLROK6aks2&R(W#&Gpj7cK6@EZ;fTdt$~ey*0;La{R_P>MfN~5)dXa_9WgYE8S$n;?nef=msDdnT"
    "D+&nILMQl=VO6!Ol5#4jnN&*@*b{V9kAp@%OLoI<gQIH2*<4pC&vR)}lzFQ$3e>VdDvR6tAE1B7nkgg-0u6epQSTZMfzrw|v<4dr"
    ";$8hlWkPwiNC@gF)4&od@-HgtRKEaY!o+Adx0MbEU*<}tqD!g?8*0#mI8^?UdRtsqdG%pYaiMAO5*5Vv6rvJYN6#<_@i3KSJB(=Z"
    "s7L{vvS-%grr<x>W<TRKWE!55Z-07y8C9j?x1=}&D2&epD)U8$;yo*5wL%!D1Eivl{HN{p@{zx`v$(GE0OqVW#t!K+vE-o+0t-R+"
    "YFvPU&KUm{l>U!0Web;viehGPZi=j{#ub=*)tK@;%i*NI{RY-OfD1{Fa;7Na>wx+*&fpth30RPJ17?m0$=#f4fDD0cl$2ZGG?!(f"
    "dBV)6b<tL30k(;&&&%%!=vOeQs;m(d2ErB~h-}Qm-Fvr9ZETQ=)z!G`x=MTmKv$Ew3ZvRDh?qM6B;~4bQ>!?KaS0pfV#`p_A!1>G"
    "RvAl~RS1vEN%v)*<bM@1B&V#^)-jdCD<ux;s=0ufL4>PC-(j~WQ^FdfybV1BRXpJ7)NxCfu<<GTJmnIHXCX#jZ4Y)`A=RiXe9ph0"
    "q(V+8=i)l>WSbUCR8R#&rew~2jm*YZ8!o+=C^E6t5XbJd05htHD;-m;(yNR>D9}HG+F5l~$KeP-9gyjukb3mjRh}Tj=~7sLfy!CT"
    "s*h3DfC0&hzs9kI5Hbc^FTzH}^Z*1L+mIH){4BD6TjiDdp%@dV12i{kR)f$Pydh_`Pd=MhaMLK0%HkP=!OiSwd%_Yd3=aNpT@h1!"
    "J(vppsksvO2rU_11u903%yKPO>^oNEu5%eHC#0OOphqI`188cf#HrjYW(=<~7QOx=HL(x;Oy*d+YL?YCK(!!o&#xk3Z2{}aUxw|}"
    "n2$SDC<-aRzB0V(m})6%E283x2Ec4-s>~ENQz9&)<<NXUGdVxS1U7oB<hGr|RO6vKI$khcS9vZ*@e-~Aw1)vqYDlsHv?_~E=uVz`"
    "=3Ih?0RM2(9F4<l<2*OUdL{(p=E~xeV1nnxU`{)$>e9P92YNMR0GQ*h*bl6rE8`ev49pVL@^JuoFgEISV$!<4WX4yElzal2HU(^Z"
    "?@eYP$cCh*wB`nz5IgE9M=b!#V&PP&!!VL+XPa6O+UqKhkm+9fQZ`!Kcwlw>0;{U;O2q@<{gc|pbQ4^{qp7GsU08;a1=?Cl3)<J)"
    "Px)iv7XXu+9R>6U=u?Ct!Cqm0G3h0NRT=;$u>mkYKD*DQA8}YFjXbU<=Bbr(r@qjfY>1BA0)96+ME*JSVp=lp%BoJU;maL=z15=H"
    "hH6wO90YF0^<whN`z!YY7MXJcfrDPma1jRBWr;BY)=%|WoJI^0;-(z5G?nzAyx>`YvG%I|<rVywVg_}(bnwgJ6e!0W?4)YY0y_gx"
    "N|d)~|MD{g^^wZ>eh2P=-KIRQD)aC%FN+Bwo6)R1)ec1^#8r7%QxF!znQcmXsRbH?Z?eH&!_Evkxs=o1Zuw=b^2#q$6=UsoDIvz8"
    "E?$o?z_hMMIPfv98Vg|6_J_k;%hnJ)RzG0eGOi$`TkJ1AZ4%C_JoZV{e!E=s9%o0d?(`Kh(o`jq8*fxU4}ftvYXmUqF%!25Te;Pj"
    "vPyW$SWc%;UsMT!!<TNair^--Wx;y^zSF=?v!W7l`6^h+vXj~j1%-e`v2V*f4paMJnWl;o_4fnVdBs!eR0|-5QHM%$H_M1}G(DKX"
    "kmUH9*h~XAUf;5l`_Q)(H#25aXGmp@Ms*Dk7hh_RQ61RD))u>}@v}<w;J&J0Tt%JpoC`{FxGnQ~GA|0t4j7&~6)d=`s*Hp*_ydFk"
    "(uEsV(+y?+HIba9^x56AZ3qW{v0dR`dc0Xx@dwUQ4RxKm6zI)80Uvi6)%V%ek4@>?K+dg}b=9ahTTEbteO~jbvDZ~zl?=+5F?~Y1"
    "LsW}O*bv`u8g756a1GnYk?!jCXzo^IIgGebt<zNFH1z4VN_tg(W6eSY#sZI<DkIQ9?WqCzOo<lpg5IzkHUT&W2u=Mse^qh>5yN^F"
    "v&RA0lQN;|z6~Y@^1G%dE$3vxl3vD~@+Eo)z+-xVFG`;s0}SgxZ4!unznJIsCn+a;uV@lr$r@z2DIsnKmBvM7=h&Qs-Yg3kA(y0h"
    "q#rf{Hr4sbb$flwPOqG7`56$(Qdz$skQd%7`~df2l@bny1%YREFxZH*Ubp2JtWT=^F4bP+a_Qk^@w9r5i@&SvZgAb2vXUw-R~jC$"
    "6?oC~&Mk&EBD@g4Tv7sH)zc8b&VF6y3IA0^f6!|<SJ2}>*k2EoRvwN^UMgdEYMxM)dk$uv5sHTHvJVxDqPXt5Of?@HWU6E^DVr5q"
    "Sv=oWy=9GFXo7O+bLS7};GAkNiX~FhKdZ=~Kk=OF&D2-JHwp9LHWiapq_Spi*Mn%35Ej}7n|BBd0<T8Mh@%e}{auw0c-}T{3C1=*"
    "?9VIuC%BCiDO8GmS^=)@foe{b#lTLrW&tzFCEHp}GuUlDAHs`JK;`{)m4{>~FGtzxQ<1}gYHno!WIdt4oG>z7jBp|4Fidr}!y1Jg"
    "t8&Auvfltrw^d%XKS&o+1}dpm4YD1_q6mo^1{HYSsa2^bU%QXO-##pvuQGxvrBu@5uTM-!{IiVaD1a|vbddanxl~Oj-1a<U$O(&V"
    "qCHYnY|bh7%gPYJ79_J4SZiLFd20W{Niv#NsZm?K0{Jkn2vu_@zPA7|AlmLGSPzDVa}8FljM!W77AIKNb(N>1w1Uihxv*8`%PpIf"
    "3P2DL>tQk#z>bNW5%;nj+(W5SQMJrqLaWOCUan6W`RViyX;oV89LmgFR!xO_Q4*6=LhjrN0-nsYdY~;_wlZDB)<yon%vH*F1<m<s"
    "F66DEOjsoGm@0202vBQMB@iw;#H<nq*dYc~)F0#z0#v0ayIUAPA8)n)Hb0@vq@;YzM?w>ta!gj)QBk;r=W=V{%A_kM_y|sJkZW6e"
    "7Y<PG9^4+3{(5H5TbAC}FlGa?SN>e;2iQ6t&_`8KOBo50tg;_Jy~=X2U#6v#`=91$%l2D;8T%^!JY8i$c$|3@qI0siVXIU<pr|la"
    "elXEX#37z`bLc8m9rmQ4ovn)2-&V=>V;_^IoB=#nI%OXLn^kdYrvVcNE*3opDiIv66(&a6;KkKxsui!$#`XSRuS`yC7RT=U0Gx||"
    "gX<LyQ4q*i$g5s4QBuwm!xJ$PfHd_RhyjT!Y&=|T=EF1MDKV6q;A?$>$z-Fntm#$ET<cOZtf{ueYPt>n#04y+G_+n@+u)3JeKITN"
    "_57oNc{UIvD;gY5940n2Q&rx(*Q}y2?ky-CASmrXVRXndCP598@|9+kCt)_}RbIW`)VnZzt-vo?SB}<00k>Y4@Ct>Sh&!CbFXVry"
    "IKwHO3leyv8Rdb@`E)SDn`ErnR{L#`xK(YRF>oiRjx%;cH>zTP9_s1<P!J{+32=W&Nfx(NUh^dM_P<b@psw*!C9f9cdmFWR0Ya(8"
    "f1l8Ffv|yA@fWMw+hKs3=*B(!uRKAfaFR0o1}<b{R6VxkL-<&3DXZONfZ~S~)X)@IQ@s8fj$e5JYI=yCUhl=fo*3Cn@mTq2D@Gbd"
    "Y}FfgslOv@Vce~N5o8juqXsQdS4_sX#&Eg;!rN{(rhI5$=vDa%sc1?(HEP2QC9mrD4jk}9g?Z316|PiRT2zAoHBnUXj(tzPKwMXe"
    "FTbp`ObDT?=SgWn$bo*!R;yykqQx=vynzDv`{4!N95mEy{!by>xNhlE<thD6Dq@pmRA2OHX3@>oU=<MeUL9ia`3?59R>$O^v{-+Y"
    "2`b6G$8PngeO=|*j5{Tn5fy35Ac78ebh@a`5YQ&$#UQg$hD%*cyi(mQU@gq01R=#gyS*9AlAmSbX)w^lu~qr8x1hKx<d)jftE9oH"
    "AM%)JEq@H^*NQQVO;Mn!o?Yh3|H~_6a_S3FeP3Y1N-5=fCPF3C1`>lCm04rOVuv+cOiiIxt6*W88Ta7yhJIP5zEFl!w-f;iwT=wx"
    "``m!U2v!^s-l_B=ehh?NC1@%~R7cog0hfdQzyOD}*Hs?(FEK+}fd8<*0Wm9T;}2bK6W^O7yimPS@jn&I5Y^riTip#V8bGbe=PSvq"
    "{xu!rU|H0=oV4{+F&%t7pMx6?Cnw+Fd=1uA&Ksr+c#V3afhluWF<u?!tBNtI^GjA^r>-up(HYaQ$J46Op7MNZR;?-&+b(r5;<yk-"
    "$vjMUA>6p6ab2apa2cC%R;_YiKJX*#K30XTsSRk&3dN$yRcS#@K@znymCIM2LhX8WxV~i7@DgUlfAwIb+N-ICRZ)HN7q_tW-rUtP"
    "1&c7Zyfr)okW<w}Sz%B9CHi&iV`E>Xe^FePTQIY#VrEX;1wp{gL!Dk!*HH0pBYG@QXp%;?$+NfyKa^dGeZI<a{)-I51wryN&oR4!"
    "VS>+`hyL-^=V=?=D`bGX9x9;}4)EFI_f6#A^iq6V<>TX@9<n@KB7l^*fF_FHWlhwM8?rL1LS;${P|~O#?bWA2okzM#c+rBtzKS}M"
    "JVQA2_-j$YtvWAG$PP7`HdD#KHYxO~$E&79CnUP><*Nu`JWU|{!j^>VD$mpZ>0&ehT*`{k(>M~Cm--etg|Sp%q=I?hPrqCb4hO?!"
    "NcAvX(-aVs>ncxC&gsaLM_;Ow;+vz1O`|pVb--{_cD@?UM&$uO4J?wo0qbgGXNuF*7ISO-07SGO^VpCor44iYaBWc&4VTK;eS-?7"
    "Jgd6yXarNll4`yYUf-&;$nd>}_qSCZy-bRiPnWQfZ7IPLJ;OvVwUcOFU4>vvLyaew;#VUz?n<MDfC3oH*4X@YFRxB{Uu&|`UQTVJ"
    "of@uS1)|z8d%LaeuuAc}CX8%wU1q@mCmOZX!Ucb{Zp~O(ru;<{0}f+rTE$iZaF0&WGr&v%v8WqjQ}i0*uW+Z@b%7YIUVax20WM&!"
    "XBNXpDZ^1}3P)AxgBU8T#Q;;PCV=>a5^gmthZI7rji%ALJOwnG+JD4bQHC>5g-KKzQxyI74#$l5Q0<M(4Bs7JsZN$Is?TIZxtdZZ"
    "S5RS?VuCn8R}uE>DzAYeVi?A@;8fF8cizwevs^QX^9oBvrn%va)#S}&1%F|KoL0=LJt?Ybxvuh>CnF={a87^<J+no<Rz|&?DE5QB"
    "(nqTd2XBhHVq;QQiQo*dv8HpZx9(EzRg~$%Y4{8k!rhEAAV^_B{s8F<2*|&4IBKcxRBQwSSeP~>LisW^C(a<}`;nE8Atoh1Y+R<A"
    "33k1Z1mGIfc!{;Di@hEP47^=&TKs-eD;SL&aLSnT`L@anFGYpRmGxo^fHA92$y!67?bX#!YMP4L`$DN@i$i944ibZrj7kl>y}kXq"
    "55<ydV5|8MNRS#8_&$~AvsD`FPkOwcYB~$!&<M8GO98dvSVel3B7o!R*HxY-MrtmYkx<|(;tVNLr?f3!SenwXR0w1hwQu}fV=`>n"
    "3Tq{+2fB(dU#-7=uwPNMqyYGUCItcp$!V@DhZSa_-eomn<<Oa^nG8x9Q4tu&3(Z&s&Sv86wZy#&3{SGGss$R9f5Vpfy?GoO_F0Ly"
    "*)+|XkpJWu;V$a*v}f@ur&2Cjz7;Vk)cwjYmB#F7T+hn32b-S3QZYi$Cu<C)h4zSiS9S#SNXngEws4Wf4zOq%*JYlVjOvTC(vf^>"
    ">g(zGZ~nwCw(_F@5F=|y2Vi#$O=2`_ZuQ#H*V$0hau>2!9)p~WN=MWf&lf1A#BF6|dO$i>$*0-d6pM0!eRV*xG6cIxAl%q}^Vf@c"
    "Ihm|wkl~2J_o~Owpt%+73yPz+5T{vYxEk(g!4H7vZmH@}6D;-yO{H7g2lpzL)Z`P)f9^OAfRF}60Z$t~c&eE|LUCC(B+L-Nd~k9Y"
    "|2@?s&`+G@;IQ}g%${O-YgNXECtoT^Qzdb=rD{wS8q`5a1KJhzkdUkTFC{#H2IBxUTm=^<wF<rJOnOD-H6EwN6jgv_&NP%IUY?s;"
    "`_`{1Z5l423SDWxFDhz`Erp{0u-dKhmwVM@+EbK&8i{D4*bZ3p*-`M+H2z1PF{)24bl<CSm(HlF#07HBo?-6xnqs2f2d~E;g-Zvf"
    "v!*&!!6+YzRaTet1(1QHpO%t18m_2WW$wGEB-*~~5L~>k^1S^iD90YSOJHeEf=E&M@PIr#f>KEIPOCQtHTlGDZy2UaCt~Ark2Be;"
    "$*lh4bD8P#7toBZ(ON1SQB9P`@hak9*IvP_swhWCF-K8~xtb=yYjM<OT~~R0nTRYmzgLHg3(t6kDU}kH-RaV|`vCuCY@<>*^xlna"
    "%X5XMx=c9ltsuv_$H`PdS={4kyU(@5Fb8C7g{)?qsSV{|4Uzqs9Ap-6<N-;k7NbJhVAV3=y2>k6+T+TUm!4qu*fC(O7eU2$xB@@&"
    "NVw&{ZcXX{z&fW2IW~&xSNE88ZI$8NLznYcSgy2pb@&A<HTBG6BTDYU)`KTbSvWM;`;)R<l}FY1W_!YDc8u3mo(NM^xJ*3@_8If3"
    "(R<2;@N+FCw@lQOJ7`^%(WirkW?Du~24k~vI}Y|1_;;_ezo=JiAlXiG07ZS)LOga08wS__pBX(YLi%^DMY(1Tpy8VauuQdZ^yTKt"
    "UNe}2iKiQPkj2d&BdiW9a}@QD?5g}JN%IypG0$OKa*Hs5suo;`vkDGYyVYYlSN_5;AJB@r6;%zkE=tL+xaz36kIFg3)|9OA(3mf_"
    "f`Y31B->H2>3U=jTMkOdooW|$&?@9CWhD{2nTEmWBdKjU&3r*~tQ%SO*{bwZ0a{IKUg>&d&x@?`xzt&nyR|_1I*O*pqWMkVdv@i*"
    "XivMsTJ&;O)YuMFtgxdhp3az=>oV!-m7vbz6Q2S0@?OoW(M@%d-e>ZWG|zUM)Z4b{rP(Xtp^PxkjFhBqK{r|UuhAtcl`5Jb051b#"
    "J9Sqx8Wz-}@=V*;;U0RSP0^}qzgFdaI5i?rzBwlJ=Ez<ng^IF%!AqOp%^=pMI&4Kn0L!3Og9T8r2kZcM!so~q)ePX%IBD#uGTv_|"
    "=KbqpG*$xM50;O5Hh`NeISQ++My47v5E6=_X=2qjp=6ZF2Xz5g*I&JBU-^=Mp+A3Su&GEjsVp(zw7N<Y)QAT~TCNy<maIR_RlSq;"
    "$viUZb&8kBa!$8pp6A7=ELN4QlqAfm2^mrpU|pOl`-Kk5lfj}C7*XSrsED7MTPtO{+g(GvuJVL2Pu0hQb496&Q{!!pr$?1jt!9B@"
    "IpbR9j?=W${a2-1IA(KNM5`w2DrFD%4?<<IhSU1UmS|N@Zq$x-!Tx-)n*Y?-T6#`YCtkHn*CZ&YFXnG9yiUKa@`OuN!w@sXg<FU!"
    "EJf##wSZwPyD2rpfed<47Xy>kkR2^5SQhpss|;YgK};WC<(Q}I&jIQtqmXf`s<bOMH`HnvSgDdgh2s~5)}+&szq*s2F>MFtT<_)8"
    "@&8OLRR&8sIA58udf(~FjzbYS8P_+!OGFk9b9##c5EWM5O^9UPn&dhEc(RgC0aL_0D(^=JmWv8VA?F_T0DI3RF&MTh_OELJ?^LfQ"
    "NlWBu-db_1T=8SD3|oOM^8>A^Nz>b~(`vD=2}LUFDWebdNEiX5=~W^RTMPTl7t@&A+bSQ5J~UNiD`Xr!nlUj9bFMS1GN^m)7TCw)"
    "yR1k@X2(qx=LM7tk*(@Gdqe)=DPKOLM6((<shxy6pkOQ4lEQ}pGgshmP4)h4sxgAhDnzWLAT^y$p09Gtlyu=kF|I63b86<gV5=%F"
    "o0@{CzoW{Zpy@Ivhuo_ryWRy9xGrisjZcyQ-t1*~QVU`^$;BL;(=UQbQVdg*UF}{Yb*rh7*%>6Pr>;_9wW~|eoP4U9(V|-?R#tfi"
    "O6h4Ca;&}yp~yagaRl>KO1dy5>?>#ERqI_~U>^EZ)^XJVFs}B+x88*iEc>xVK3cAqD)>H}va7MD6t5#a%3?uRAj?Bz%67r3?E&OC"
    "V?l#5tc|?^jPU47Rb!djH6lc>Qo{@z0LDw)8!O2TZ!ibq1i}DNOqid?u)v^{H8a#a@z#osRUX|B=fv81R!~F|;su|SqN`UD%41F+"
    "J3RxP#0>BlI4WiKTEMrUv|BPBt2|iQA7@#HshVX5mD98^JQJG1YI=Z!swiI(RE809Nn<}EY;34Zyp)9?TvvHTeU(_^&Lpp)wy2iF"
    "LP1i~NGuhUsulLuRCWhhf@qqE5UL~)6t4lvDr~qd^C42Iu7Vm^qlyU44un;}P?K6q=^?WOy)-Ixg%?`ZK`(~F3{B&o)mu9Ft6Dy<"
    "vF9;XNJv$d^2mUBtl3nQw1%Ov$5;l{1E3;q<<qLRX=XjYqDcg}yPF{k&*1HNObq6`F0hiu61A=*@1`-i>QB;>Qm-`L4T^5<&U(EF"
    "!iZ_Lfh@->B9E`AUpT#9qrvi-)ip~1IuZEvsEn?V(A5ZbiK?3d6)0WC?a?&Y1SwG^%B|j49YJ0rvE|2f3@D7=i-Mk$k>akG!HU7D"
    "PMw4rdXI8KVgNCHZpdMdKEpLzGuLHaEroSt+u+!DhvDNPa|gq8rStPg%}db?fC^~G$*VA8r>TJ;dZg22&V5VxQjem)=v~v)G{|eN"
    "OV;3}u@(OZ^VO(2Kb3Tks5hJfiv@+NItBx=H0xQs<Yq8mMN%iPIOyF<B`LO^qHNsC)@)^nQ>rd0OIk61uM*6x`73&L<PbH6TO~%f"
    "Rh}%F)9e-3^0i6iR+ZG~3k1D(i@nzd4$o=mJsgLo_RK)cAp+-B?lQNs8Lq25RWb@;PI<{Sr<x;aDn+nkDsMs+kcDNTFxUmNP=b>g"
    "4lh^gBI2=Bz;@kBFdyGZj2d^(ewv8Dj@0%C-Ow9J0&2K1Yf`P+1pYv>Kw6Q%>yw##t3Oq~t`Z-oEZ`ThzNe_S;L<UI7_zPBDXX+%"
    "JtYmNSrUE$L^N0&ZM6tM4=O4cH|>wFT%6NTI9HF9!dR6feX{P>*^pQo8^MP|#QV|=!EuJls8dhWm>c}xt=w0Qw4b2wsa+H3SI^R+"
    "Xth&knT}Rudq#?29F-!^#?<V8c|`~XK$Ozs#b>Xp<X49@brg;|^R9aSsiB(|nx$c1(<tT>mokjhC0jFVG|ian**Ke?RQqkNakBUj"
    "avSuv;5@>ksTx7^y%q!rUBLD?uPIVyJEMuXryj&;Ooy;IXfY|TLL<^H%cNJi=b0!2z)-rEIyvSIUf#&usm@UoEO@l{N|?~QR#%v2"
    "%7iOhLp`9(tq&UzCp|Y4)a|7}yp?hjqNWy94Uy}4I2f|pWi6#mSFd6)%owl-FzSN>oKsb@^SVlX6)&hm#$MZv(0$g$bNzrcqt_Gm"
    "0I^f{N=K*`c)zF^FlqQs$B)`>F&H=A_ZnPuO7RjTN!@5P3Eh<0ktv)P$jqt}gL2G~{&i}jS5>$Vjm<qd^@Q#K{M#z8W?x2WC1ok8"
    "&NR-)tE^W8YqGitsHt=Ar&U(y>Evs}quGY~PN$re>A0?YiLW+nN|HIvJECIVzN!Fb!1toESk*nQBzssiCP-5s7I%0OB!C}iB?5A_"
    "%jxMUR-B4gvmRdHNVq7q;m$?VZ$aasErUJYeg(a7sZHJ)Wub;DYWfvhk;l5O^1PGS(;-1N5XCk_xiztC5~@Rd)iE5)B{_gbRsC4t"
    "b)JdI;9k%|!!zGBGkts~(W5Vp60E1j5}M&Xs~Xds&TL^Tnj9hg`D;!XAP~T;%uldiHOT@6uLAIlEG`27rb76UT3frt$z;HG?5LwA"
    "jhWu2-mIWSXQ-8doz)i9*O-GyY7G^xt30GkzO3oB2TRtv#w-`j!GqFfwdOHH)p<@Mur)lzs0)jSdqmkJ&Ft}@fZHms-o;V49FU22"
    "2R-^k4RmiA-B`UkQ9O5MgMg?p%~%HYE3>E;#Kx%kmtR--m&$7}vZDT&b`6f0D#Npy73(on8ix*YZ4L7~t-?&QXHFe@VKUP#HGnC9"
    "12LugDw0)j8BmY-WaYp+09sMQB3KP7vQWjYRtQd&v*_*6IBD!qI86uDymjA9W`3#{)n%Q{u!*HEnR@HVd$U$j_CP4gy~qBq+>7S3"
    "7f>vw($v#OYnK#|@2dfLjmJ8b*cz_VPt(|5G%AIU2dfy(n9`)xrPhIYa$q)V>KgK$gJzp)$}{N6URQY<*NbD=td{k)X#hF|RfCh&"
    "*4ZkAqW&{|%n}TSyH?HlKw9=%yTlIlTYKMR`Bl6U{Hc)8a41b!kKCD4+b5$T(FnF$+HP-E5P@M{O9j5re>@!2dILg4GjFTBI+v+<"
    "6-ohmkRnwp&ZL~dY*0F{scucBO#1rZuwl?mWgG|fQyH*(K-0Re@=TO6`OJFSKI{Du8rIa>PmRnuATJaZJPZxl65m&AMm_fUb}~17"
    "sxZxKn$&C5S5&MltJZaDcyGumti-8vCU`Ox#X+X+9zx)$>EE1Y`!2X3{2z8u*L{@-{&DU~#9ew)+Irt>K~Kh8aZ2I7xsX|0oF?Lo"
    "Xw{YnH^c^1%M8#4j>PMcz2bg~VQxIUdaioq40XwCR8K6-yJneg@mO?6<$2bjw7aPj34jE*td835y2>j}Lh!F9mG^Qes!@OgZE$cO"
    "io@w`^o--u5FoTtT{qOXIID<0=<Qj-y4x}jmaUp6k{$49>8e#q(32vw>rAie0!C2DIgmCnCV%=_fl8BF%HZtU9Ok-8dM-w-9tXaC"
    "Iah6zR}oZ&DDR+&tg6Puuz08bTh2Mn6U|Pul<?3XP)Jm}u2P?iIUNO^#8as-JuAkb`5~&pn}W=U`BExAcc?Z3&}gfi#%RPrREm6K"
    "nE7cbe_FlHGBxx<t>S^OGi0N2xT=dP4VTP^nq|+-&jw}}7*$Cgv_oBsO1#>bSAo1D4EVn~^|)uOaH>~p^(5G=_CYghpHML_z%)Kv"
    "O%f~Ew`n-=7&~51>}gz<&R+2A36Hsf%44Z%WXFqOeinME-dlWWtjiF~gky@~;>z$tal)<3jmH-AVVbZWFvG?n>!D{kjSQKmLd8vl"
    "TQusFcWK5C3>1QZxeuCS-_^!4hkP6K=w4+wU;E=#n6{ED>uKMo$NVwR44jH3(U2J<hG`Oef`$nmI%}1Pqe=4l$}!7RAzxIxGM88P"
    "xQsnkSN~HCrqZ|57hP#k6}5rGC{?#*jyD#Kg;tY$cONI?Uc=GCDfV7^BdU5F2e^7=l_CuTtHN#ZlVUY!ovT_nWbQ*PX4LdwZGI|H"
    "=eCM_wMR!|^t#8{d!dy8&)8o>N7>-P)ct1_g-cWcAZEp_ZFa4hf~9WF<hZW#swov;!XqJ22pA|Zx)PNVsA=R1$p9^u>~LNBOuhnq"
    "O~v#BrvocjCF6~j7w<pz$5G#~DTIkDK?b0kUG-9Yz$lpifuA&3n{8CWqW;_(KDpH)54nS(-&T3`U_KQR;D@F_g$7<OTzb}Q;qI0u"
    "tZ$XGl&rHmwS~@`CV+cLFX#=%aI=^n>V51<EJTF3@EAFysQ!{blsMF!rU41?!5U($J`7d~AY*8bSuUC>dFlkOR})`;SwUI6<cx@A"
    "QOW?q+`GbMEiRQq1#~kH)e)G(TbvqcBG#&IjCtjC^UJTQqUg>@fvF*q0C1;TdED_fTRC4zkI6yrHE4%b3Z`2HxQ@T*DF3igPxM#C"
    "d?-dbjqAJL>hbEonk#2dG0~11`DWE^(JPNt<Aw)6_gtDD6>+I033uG+Qh6Ge1wE6iU>JHelya*gZ;qlA7AY!{gUT8`aS0J9U!|Q&"
    "qSlPn#8OS<x-pDWUuAfLm@Ot%U}TLy4U?k_^E^=DdjfW4s%tbdoYjp+nT1JHOt69w&*-m_-)de#8ATWzK+mcWb;{OnRrzY3M1{77"
    "nlvHl6s{}zaC>aD{4~T(qfWU;e_iD@O-jXn(BABTVz3i%kh(={8lv(QVXVTjdh#n8y2!x|jb`aMeY4npiq`;ReY(~M8Gi&DKC-!^"
    "86x+cfnH8Cg)sFn)}ha?XL3Oq>iSkS5-sC`SU6y^Z>u~8GYH2T%?gd)NFAqIu&Pj8<`}-L)ZZe8v}Sdt9x%SZvx4LR-l}dY*HvEQ"
    "1hmZ7)JV|Q3R{dXsZOYe5^z{a6?zeKtzp$mMGQuhOZWvfIXr>%a$`%c!DwgxRuhaw<<%>6sYG}Mb>W&bDNX`QtC})AQq(w5u1)ib"
    "tg7;sXj8dvCghJM#<ixNg-(@9tr!J$wsAqFtLiX063ztstOpaTNk!JFkD_M4f;{0@w^g1-zuue#dG)%MZVwG4&)tfMS9K5@M$<*G"
    "ycia}D(HCu=89)yqu>jrVc!hdYy28GUtJ5)E`?Aza&<}6eD%{6M{WO5Q9T5hGudUqz>+43xFF_J@M`Sv=KO1Z)nF@*y(CCQt>}8x"
    "bZXSIDV(fc5=&#4rHE=Kr_TSFYdrv~>ohjP1E{XT{4|vGLBlbY8kOxHlSa1;MRiY0Y;}F!nue&!LF&n-NgP-+b%D!XBcCBTX{GCK"
    "p4@^U@82}uC#o9-M8iXwRWcyWXqZVsS<U=I+N)JbBqnG&jfHDyg?i_f>ngA2h^K8aRu;IT83<905~kCegYd<+#>s3cAy0iPa4}1<"
    "IBzueQ}GQT*VW8CQTZx4Q#Ga46R1;Xw}Op9dyuY-lrO7X2tDvCudP9NG<HqRhR$3@Yu#;_s0!3xv42Gv)w#J;6%r^(#^Q@-Q}%TS"
    "L6xGs%&GyB*wQE$y2b<>s!>0NY1~HRsgL9Hj45kYRGd7NY>bfxv)Iv_8ZQng{ye?dN@)O0++EPiiVC#DK4W2*y>%}0uhO+MP$ps!"
    ")oQ63eLskR;E@`k%3lt37THUigS;ueIEx;a)Nde7)wcHQDvxDW9=7qw>KTflV2-g$nbl-(J@u*sqS_eXFo!Cmmy%j$Q`B1{zI(ZL"
    "dhoAdf?rPzd^M(8=Ei1}d0CD}kr`EE9!+J!25lDW>c<F1m!TGjQyG1~uTnogFY~p}%m~1Lu>uX6o&&PkU^NY0c3C+sF<MnhVDQ3e"
    "Sqh*jquFcYI&U72c^dG~9HKBPbQ`xEoEO(FP2(_Q(}*<mm}63Wj2|J3RYfRUL~z2(=4}-hUc;o-8x?2a<?p43XZVIpGYc2)+@ASU"
    "id-oC-j!0?$L<@~6ZE9Hx4kX%q%T$hCXQXpaY}l8)#Tcs?yFwSqfr=O1qTCkU1xM%)X)N;0+da32^4T!<?&@+P3spZ%Q>a33j)Fc"
    "mS$|A-Wa29^wJM{)nsuwiIEXRt0FUvGsvst>ygD*MR7ipF_aml)~!=HW?F|sRKrNPHyi9Zbfejn{NgNnTGEfej6qX0Ze!m4YiJ>~"
    "Oah=RMGdaNqGio>0-kRK48yOIOEdT|b#@)jbgKs~rTgvbGYn4l`?|~nGWCkFRH1@+ip=^B1LSX%RuEw|PQZ(}lPld-NQ2jFljBn3"
    "+EzLa{FWlmD*j_?new<)Nrr3%t6()|+1kn$)&9Aw_}df=w$dVYjqqtDqk+v&KgoIB%Tpk6<~*Rz9&6mx9~l#c<K5YfoKANv4W>`a"
    "JKT2CM1wAph)AoI@r3^Ec2)n^Y^*a)9vGzdQ(j7svjnK9yjl-TP2G%CCs{!m%53BS)y5Qy3WL$*55KSXpZ}r*|BwqQ9ltm>2+!fb"
    "oDpz3dB(Ok7<<zQ?Ae17oiNm}rR8!}+sRw-$G@umie51%Un_66v8zE9qe+n)8B=rkL<PDUEn+Ooz<70lY&X#Nsi2ld4yNlW&y&0~"
    "29s310_{M5lffEhmBIOGth-C<DZGbQPLWgQC*%Ts%IcS9h0NF0$kdm{$d%#Moywg-cdGO8O9MFd7_2e%MSTm@9mT3g)~=e9QTM2r"
    "nvA<^*J(oDK3!{^q)da+8Xtc;s@1Hfa_VB7YAVQXE2{BQ3>)8-lu+Qu2&$Bd3t>#LTvvIz)~KjNk?Lra`KP2sJqrWEoZNDO7g;t@"
    "hB|7-kU^en&yY@^Gq#S|v*EhR6O$Fh1h32`5ss?zSKC_vzj}M9VmW_JTs3OZtFGl~IyHctY`?}s5pF_eKO`54$~ewP-F$T`c5~|Z"
    "cs(HPGk-zNO94qpgPOnL=rqWBne$q#af$i5%roNqJe48C%Cw$(esN9dovW#{znT;`)e-Ha%P)dv7$RiYy3-pKZ-tY|*H!FaR^eiv"
    "<u0Q+&HGMwjFT;Cgub&HwvX_&<J?YDor=J^V1=wv4!0?9kWcfmXEok<tmP&jC6tXdt8j?fHL`HP%r!<tErk?<(%3Y;IOAYckGqte"
    "bzS9&V<_^|aU#mT*X;IWHSq(u5f{Qb)s|hYy`wCa^;pNGBn0I#4z+kJv+nDOz1j(>2D?I8aug#fR&feF<+g-_98I%0`kA|+ut1h+"
    "$O+P;aWH*lO-YVdQGTj0PhPRct_MA|XjJG{!RJt|zb2P1AYaQ(;4^L8N6;u9i0HyG134624cAp(fxjSsxu?fn@L^5;@on7AXuOKD"
    "%zeVMgrIshjZ2yUrn)sa5Bla0AT+m@<(RMfah7mtor~>GMX3;Nbf#)d0%!>rsSc~kx{mck9gKqLVDVvlSL&bf;d=canilIxMzg}P"
    "w?nN#5Cflva=NSLi>alPE?<L^VysGCD==hJ0J=Tw-1vI&y3CXGr_@3-z23$lQ4wS2a8}(RMiHu~_86$5-7dMQ(%2#Ro)l7>nhfXg"
    "8&@eSf6=n8ssAJwGf#I)xTD&vTBoin!{PWbk#QW}WWmY(yu+zK9(PxU9akIk5KNy>1Fl}w*d3&=%(0<KXKq2<j6z!(&x1^W)q;kK"
    "r@>uhz0TFHDL47n7}3A#7P-HZ!lY?E4yF`b5A`&V8{hy3>{LfSWymQ*7iGts;<_+mDt6<z{4Tc?29|kVvDCFf1B_L-Bu|Nwk1d*{"
    "==|=dd9Cjz2W`P&ED>O}8Q`0$d9r}Ox~`I*_IoPB0#QT>r9b)eDkQ`qtA#{LC2IHs|8;dGvTIsN01s}EJiCMdUjvM4Q1Ph0-hDK!"
    "8Vjuc;dmL$ZNN@~{?S*Xw&(zt+1$<wDJM-|?&{48^oO?tZLh05Q!%~TiGYO%ih%(s*ckD1Wo2YN{wfhSa9CNVrto5z8k-bQvW&Bg"
    "h}-qV?5mZapr69p>bYRj(AWqy^3%}0sWGwDY6doBHEXnFP*1!yoSqdj#4sTEHyh*J>)lV-Z!1bPG-na{S6?k$=hWZ@xR+BmEW6ce"
    "EhR{JM)9)<Uo>MJV%D#+>|TxWqekzk_7^Z@mNITN_QxWks$2r(_gOiqlSn*O;;(pmUg|#|gH;jT4g0w#QKE<~W4j>8dBcSVcvTrW"
    "a#a<}nVKu7F)mUS73Rku`!l}abQ_F9OV^7DuW?0U%T|SMRmI3@AV2EBUJ)}KqMic11|oq9^qh!ShVB5zu|{i3-Vx7uU8a2OStn?X"
    "|H6}i%9RU=t<jK>r6Pu+(LOsVfhtT9%chcb<s5jusv>&V%{<w7Uf565jZPwwijoyZ#*%m}Ni3;&mHP`e;Vh?9$w_wk*#0Qvqftrq"
    "w#tV@i8DzKdBV1%IBKqpmZ%vIC_`B}YF7@{QwiHHE?GddQFaTUK%Hk#TFY&j$1WG;e5oh$x!8o{;%_uJC~@=Z@PdFFm49xfw2lhb"
    "4OWSsxJ^y!)U)i(`@8y@ilU1NFity{DlGLpro*Vp9PZ6%wC4Z;z|FG82x6fXYmaL2F*LcxxT`69D)5uhkcVV6sa$EY#;R}(3SO-m"
    "`gjGn!&VobtcM)AewD3fuhzdI#i156*HvEi<T5g9AF7Oa$ZQG`8UR+t>NeO|+t_O`Qd7ctkZ4EMG)zr)0rNu4ub#}m+Ha}Fvxhbg"
    "^@Z4d49z;W;AhTIEonO&Q2;=FS?ejd&OpMbT6r}!vSHque6h;&x+EN<&VxH^NHza=Qiow=y#qO29$9l7f;t*^HG2aLs%ov^ZSX}J"
    "m2+z(>R+=;v)+7b5&u@rdeP_@KZ|l<>Qn-xsxsKAI%D~1p+|cwXUXe%(0Islyfu<$l~<*wFgZ7`Q3_aqa0cD?nlxewn>p2EO;Js_"
    "pI+_Y?#7+$1un2F%`jhC%&Rt9<D>A(0}~lflZC#WimI_S1sCB0WK*M%)U%K+H?{jPV?#Xw{Yk^pfeP2nJlPmgI9AOk)MgtJ6TxE`"
    "G+>zWWFl8>)e<?VEA0?bL*Gy1#G;t9KW|axy2@)N;>r2aG{<l{5_yj=^eVi^O*Zw8<94D-=ahzJotl<<HJYs|$I0*lzQ=7D_saM^"
    "6TW&yoB4K-Msx-0uprIhOhG88rxRcqur{|+q&Ge-ZzZ1^D9MV~_|mJ?`$XixK&ffIX4^Ky{8ewKJPSR@x<0U1kWDBPPR45ZR>&Y8"
    "!9?K4^!|D?ukNc##q}Db0>acAoRZ0VPnt{V)dEpiMdNQ{0JcMER8_9&*m%DrGNsXvw^d#a^9-vFy`geTlfi{G(tyLjkuk#CtJrUZ"
    "Y8u=U2e*s?$mWRUuV8bW+%<*CzpA6vURhZ`SbZ*3J)NAo6$10v1#S)6<oC7)p93~7WqM^EXf|^OK^JvkyS5VLVjhp_-AVg$T<oau"
    "yaic8akvW&KnHs*jra{)HD>T-uR21Mcjn^7aKLi)zMH3CwKr#uSnmKI3<C%fJsFz73E}s3DK!FH*K?JX*x0cQgvaJ)+^mK&Nn3BL"
    "Jgw1AH<@F?faV^!vXn&Vz^Kuv8-Cq(<S~Jkd~1*MD)@XoluS_DsmpNdsx$dDut*QTz<KO!Uf6$@Qq4FpumGB4-W!EXYQ#2^#c6Y<"
    "rD!f<iJ<w>S7`CN%BwlXX(s4)18#W*858ibfQ!`hr16PW4yZPP{I$C2yD5({wEW))``cyRbtBmhz9X9;@p(BQK}0wlj&Rt&-uLP~"
    "2DbX)RM?RE^pRTHYfVg&Kvia;DpTi7fb~4K@xOk%%+k-$kB7hN*KwG)+#og5ec}V@N#9g`99A+;OU?I@RRaqC4~TwS#=L&%zpN>#"
    "Wgs{CI4^l#!*DIOv%;Ef8U>U{IuybwN4C)Xu(d6G_jZKd?(Hg{VU69muL9`lU7~@P6~)^=3y@MEoOqZt{WyZE*!=UNR)ZYWYg3+V"
    "?5<?u&lgASS~ftvy>V9Z@N`U>u$$)Ta9qzIw~>w$-g)+LDY|o=nt(v6V|^SkpSx%le+q2b&3u`;07kv}MQw?5fK`LkOyrs1K}&VK"
    "Annlj{+DBpB)YMBKzG)w1hZeS!~&UJYioAp5{<u#_rG`2Ip%!2{10ez=uj2TF%)FhMwW0CwXDhK4l&34ISJ?^dey=4K1hOU-}AEz"
    "puc!)*>xv!c(4KapLL_IYlg)4X3MKB6nBl1Eb}u(^cK^c+ah!h$_;1IQv%BGUBhS@D4|iE?yL9x<fDfhawwW6yg}#QT}#IGcdC4i"
    "ueBworR@HzIf5ib?MSdrv#;&Cd1z~C*GLe~q~Hv{$C0!X+O-zj<o3gVR({J#rg|NQg9SL7JnfECTl7u`Z!J8*eq1l^yiR?1mRA)6"
    "U^ZA``(b{4A0;g;tGPS73!ni^(&2U9L`Jc!EqS)@DJ0I}b<n)@hu`8oiAy#eY_AtSAJJ?JL)9Iat!!QXA8QwYJ}itRp&Q}x@T<Rr"
    "(4(5bth1=oSe=jR%CX(sRlaV_V>RUHry3K-BM`T2>+VZpMxYl6B&^IQfzUYfQQ}y_Vk218s&V^W{&amf@ilo@09W4Boyo&O7Of*V"
    "^<yf5Am-G`AU&^|_%;E)Z_fHIDQvzCGQa-yu(}J>y4D%co&~n~?Ui`6NFPZr@XEI-Ps*-4$GRO{65ZsuWUJA;lf}P28_~;$foyZi"
    "34WeSes!BJ*j-@HYw{we?uC%`%OdugV!hxvQRb-vN!@{r^mUh=Di+7$-Icbz#aRoz6;!Fd!%GGL4UE-)O*$Y8<-4xF+0*f}z%Q|{"
    "WToGf<eDrP_FI;f@vU)}r_SL_{z*DuJMhV@qBy7st5A8!F-BYS2-P+2B)G0`5<5Fr4DusKvN5e0B_gL)5QQsRVaM{lG+*bR5mJFK"
    "x^WgZx{^=pN+@hs`Bmk-U5*;vUb7zuIe$8z6kvl**H_rMqfnLsYgZBfb+TXtd0;!EgD5+{f-C*GkY5?IEtYw;*)MO5&$*7KW>wu8"
    "wj(W{EshbWpCn2QPj-Ild!;7#c9}n_5~uS^cdMsktx+#Gp4Ab*TlxB~^rN;SoO~tcy6?7XmaYKXDm!gaM*0D>b4jCG^tqNgnJrd{"
    "%%+yGg9TkFu(5`x`RdF10|I|5yL@}6bzHNh?V0&pk%NLCx1ydkdr_$?24q9apIJ1+I{F|y@L!TAbtqrzf=3bku^|-c+v&Vor1}Ve"
    "dFH&{1Z5g)x88P20Vs!-OOJCCzykdW2lVe4>sBTM1nUWi8@cZqzpnI?uUHx>tGH=rB;UShAP4$`Q+gGDSK<qJd}u64Y=O+MJ2;@W"
    "7)Bk(w%*&;d4Ii98kP)bq!!nQCL1SIsW}k=P;n%VFHAU4o$(ydPnNZ>K&fXHMqqRMRep5_cXQIui`aD>DtSd4^Wou7EO&5I^=`U$"
    "21j>MC3NZoCkJjgHxCBht@0}X#I)=;g44aN^4666wPbz)kYfT(@`X6$v%XDZ89v^jEbs9;@kJ)fVN1;N^|PxHWb&6uD?>3{l4rMZ"
    "&5=BZO!P{PstmqCKQSA9Ss;ukUskVMdBfc*Kj(ETSGGzj6GDoIp+6v_OQ{!!KiE?70T<V0=m<IF;5vjzR6B^wH9~!%EW2eslaDo7"
    ")`_pmV4QaJD<-L6oSPId!G?6;ppFuFhEocfH2}!>7}r0Iv%6J(U?w}i$ob&k_15BJO2Wv_g*>c*^Z<=RYi*8JIhs#b`gxumB+xxi"
    "aqj$TuKe||9^U=USir-eZ8nwuO{3^{m~3lkgM(;UFu!&;0V#nQ07IjydioOc?ZedbulcUl6eCOpNeBcaY4T%hieCdlA!cWE#02gM"
    "rE)CmxmO8Mi=Utr>~@t8&BUdQ4$tI(*~Ob2Q%<XO*0wJWY1QeyS?M6+v}dxrfY_i9A~jTC+bVl~Q?f;eKTxT<Ihtv9BE1GIY;w)m"
    "ZPC@!Su5^j`x*ywC~STPYsFC?)9tm)=U2BpJ}me1qWNuM=ts1@x5K4R+sGpO=@o^t&jJg}5#~BKRyB*}YrC9T!e=CDT!uXWj#R~-"
    "&e)t5t>+2%K5u<LMC)?rp_A|(6-R5lD>dP(4TsxT_8EyTy(gQl&%4)<4)WQftPAKuKXx6-YVa<T)2S$HcmUC`n3oLTm~?x0!Xnq7"
    "DcO~n8>euBTshPslRX!^tiwvYyZTX+9ek#RnbTRx!igU6!`a52#VY{l*O8@i8N92TVvBQjz-`+y`xtg7+vM5u3u3sQr8VPE=fsW|"
    "3U%td&Pm#?@^4jUZ}$(ioDx}CJyDCZM&8yrBodDdc=l$wYPNEz5+A37G@<IzvRkG6xnfl4aYWq?4iIbtzVz$R4Y$A}*wQ>moP$tD"
    "NYk^lEo0mA*qyiTg2VRnf30#>|0IxZqSIz<M{<GZt#f=FUWCPvysqng1cwmIu1fz&gllTUsc*4c<xiu$UH^owJV@{dDe%oh7a1v6"
    "Rso@y8VhlgN7Lx9&05%Y9Ym_$K^Swp%GboUOg=!8u%D%e6*)MEBKS8*{dl^5k%e8Q3kO?-v>Nni9-1=Fcn2FV(sr3&V0Id>D|0@*"
    "DpS{8Wz7_haomv4{%Y-@<=1Olcz=Qg-@>^eLsyH$VW(PoeFkrNwK1H<emrlO0NdKk=(Kj*nz813R^d-m7CkylLZ_NvZ#wS#vYTVO"
    "%5O-%+FY!pJ}HER&mLI)Igips3ZU~H`cZga_mY~2>=C*{;EYM&<DnbhZk6!C|K*Ct#|0cs`yb*eYeMqpQkl`WHd~k(0NFw$(_B{7"
    "zY2yWY4W1CP^Nx&8B6H5m9uyQpBMADX@oZf^}24S;Oxf9ma_n7qd}*EN!~mHdbAU3TP#yQhtFs+BP+Lh%Ie>PcucG$EVp1~!@fB6"
    "gYUQBq;CF@?Umh6Hi9EG*O|5(AbviA%dXr$rL(CYr;b;8K$Ap*HrecXs$LtwKj#=d>$*LjS53X=csOTw*sk)EuK`-|EYW5jKtBA7"
    "L{CJi;M(rgZ&U6eL>=?^M;E{xECBS(?7Em#hq-pXD6I0U%U0XISD){DOd@-1I!F!G7dwX@3IbYIIosQX2(!Lnm~+|5x4Yl`LOXHP"
    ">l?<j4M1&VD3=EE{MI&OL-8)vb@@-QK<zhI+Gpvp#*UNyq3p$@fK89EeV9*y+j|){mXhZlZ88DWSZ}ofoCW;l>*!Tm$Lm#Z_DYJG"
    "S}Wj*)#uPyc(=-@Sl^cQB9Ud?lG&?&#*;YJ?3KvQEcGMa65E4=#esQe%C9YNRevUDHnvCRmt~eIVz8@64pxAU%N?CGHPoqntqMYw"
    "p3{;-Zr2((op-!>7Uwg&k~7;?>R%03XtDy8?6r2Q+^R~B;~A|V&N02k!C1=<js};LH%b%-W@;_R*56gk>MzNZu!OikU1yKC#i?be"
    "q-$^2F-rHF!l~e-*oa*o5L!9sBZdJ^=SJ+=ewQ!F9Lo|lS(ckl+H_)<5{4Qsr$~ekzAF{$tSxo`5AmLL<HJUSW$<Xnxn1UW`)hyM"
    "oScFJ5Wm2*y4m>45_G}n&c+jhW)5trTMY*CnUyDNChBRj%XS6R_J?8um`L)LCUicqCUXRMc*tIhHhZrLCHDpLuZ}t0RpTGW1N-qV"
    "lC`Zcw4YaTvmZM&9q@n+XYJAZR%ck*)%U^kg>kfQ3VJyB<c11WdaSPJLWEe(oZD5ta=9Ka_I(8pA6W&I$5w3@As4DgV&ecg<=YhJ"
    "^+dA2ADvMG$v-*Y)VHhr@RIEkEkMYuA$GgQ0Tt>Y-OE_3AHWIkAL??HX|`u`K5KrKrs<OH-jTBQwJ-}Y6o-OH98u+3J*hOoy`5-L"
    "HRolQje7=Y8+$G(`|{dcM2H~^gfhZ*mEQ$ZHO6r6VZnKxaN|%|FaRPn!BP)FHz$}E8{s0I@dqq4mH`7iFZ&+)vHdVV>|gB1o}}2R"
    "-f=ep93)*52d7jo%%U3{K(L&nUjkjnpH$)nTX?xlq}_g)PlxTajg1g5iiCIbc^|&Vt)w66IiwewzFHDAb3TkWY+v%J!eyrs;k|dh"
    "yS8n8P0Z?W-6g$hIh>-dTMBtirQ~!juNt7TsQ`!;<6mVurd|`gWqV&{dAG}aUiD?omKCMEz><e@KKiLN@C=cStpUS*SF%*+&2Cyp"
    "8zntNgo}N5?<BadZ|e725}8lbeBsd6kKKH@-UTC^;GvXOGS15fhvV7r`doFYk=dh?DtpHg*xeslIZKRe&@-nTI&ZJ}Wr-neDU7H%"
    "cVwyNap_pj8w2=W?irgKTDbf6F5~lF%yt-(YlmvYyl@WVc%a%OnwKY14l9$fKuse(r%L7o=)kLe-UIsk4y7-)tNgTUo8su!B&{0~"
    "qT``)7_mRx7D?L#oD)n=5{mk@uQXlDr$?-$eqt)Sl-cUn`0t?nfRhrMFFoYyoR?jIY2G@tpEs%g2|CsQ2|%*Pw1Gbn)S7*ifZe`b"
    "rhS?pW;4EnTG6D9q~J1ntWTvF+DXXqc{n)=8YM#Z2{P!baf{FyZw0|OhckX7ti`gd%>vHzh4I+Ul%g6^&&}C+BEUlZ`TMeMy=zkT"
    "<l_v_ZJk?1v+b-*B)b0f`G|m6J}kHoBA%5Lx!`@?RqO2OU2h&LXLC>wbxxF>&zvU7K;4RQSn>Gv7}#SposR)+CB2&oZ2E@O8~k`@"
    "QfBAC%QG^mEW+6b!OYe?buAyIAGWJ}R~c26l=__6HnW$Amby--7XQ|eHe}j@yi@E1O7XgaW>=XZC~3<F?AwR=(%Bj=Ws|+v6((T2"
    "DCp)e9|%vK=V04$xu-JIe4l!0pU5v#XO$d+0CU)_@|oi5i=>sRy;xR(bd#`|oEXZXXrtJArW(^2FF_m5x!UKe(*LXh9zD04EdN{W"
    "pFQ9hWP4R7d&ph7cvQQgif;17xr=!ffG02^D+}r4J=JTiYujK>F!@z~;%dt&U4No6@*r8l_W|YSt)Fv!vnIbPt^#X-X(QU@?68C*"
    "o4M=5{Op$btOQTrS%KoKfUUUN#m?=aE<40mL-+K+!Zq4}=Mf{>6Bq2=I+Wrg@V1t>%Y4J7>t43;B%p@Dj&&G6sKL}NCW-&b?HDU4"
    "VCv8dyM)bXl*|7~j7~d)=NSIn{w_I=JDtkmsl?(%1sVHMXB=h}bu3MAf&7nk3|Wocpr*=c9@j3iY*+a?Nm`eC!1?vj=AE>q=JlaA"
    "`MllC*OfKmnq-0Hw5?pxAoDy`?VeGs-0fER9sgy$%p>%<#%^H0jz~L!<wQ<9I*VHP*38uEY}5^>g=^a7=rlNtaQnu7H3rMbt6xfG"
    "|I03T)v(~K4wNZjI>FQ4X7h2XN_Be3sXYZ57_XlN&U|l|X`e5iO{N-aUp6uF>jseC;03X-Ebx(l8~h^jo;A&EBcDTK3kwp<^*f&1"
    "W&TX$*)O9oN+UepZqZH{t8v;ftz-r~|K`Y?C;2TwWsk&*JdXRd>{0DV8rxO=H2<_^TrroJyKaabt^*YoZTcNnkE!OqCkQH?E&34m"
    "@j!T-0zK3M*e>(a`wXkeB&<eh&iPPcuP7mW-&48dYiVLS@~u?g8&7>v2;&hdQ<L4=yJ4vqzJ95t_y@<(cZ)duXcPvu)TjW0>D;}p"
    "!5V)Z{P?`441%Y!tCLWB1$*vR`6QC<mmSK7YWxe1Aq6NNUsw6WLHS!%;RH1v@ve$zrPfXd{~#d8y)34;t9<T@s^qD39{ItYpf!%<"
    "6lOSAc`sJ`LD{<l1WI-xBizJ)pEzZJx+7E1_RRcxKhMMAa*l>xB8s4d=9R)53m=*k@B%=(cV%SBczcNt8P$`kKh8Eo5LJuZE%O;<"
    "l%@JpHK>l>vS=-$$7j;>^&;AL>*jB<l{x1U*@5HcUoER0)y|e(UpA}!tg&RX(e?;X3z0tSTX1sZ3Ahg7^8wb($56`ZyzR{e>VnDP"
    "pEchHG<tW(7{dq5mp=+WQxoS7#LRq+(V-l#23U<<49X8`2`lvOEL-43akR36Ej+Wbz1w9zD`DN_?2u%`@=GeQpt?}G|KT<yPEO=d"
    "IGYm+Cp(o-QX^Mo({KdP`n}x%pGNXwHS1O{Hj!AABdlPq?5$2dmCboooT}vC`-Qpirh+r|K_7M{;B-*jMzi`|GkD8*D2R41uW0*k"
    "4l-5D^R{HFs8qarzuR%y(8BZb+(#pCYC<)2;N6C^4*^t{slIaEm2#O5ker-)ZPs%IOT#a!T^lh%af?*|$IyCty!|g1bZ{GJeyVRd"
    "{^Rp%*&tkeVMQy?TV}>d5n!H$j}6lIO&uNJpX?ChTgImDGn;LP&BAYD2E1>gxq^&_I06S=M#8JDs@Q4h&4%i^<+{6;nYk1E2Z)B)"
    "<IZQBZ@5|fCK;nuGticOYEBe{Gy-7rCH<$a6BEEA*}MYBWIL>xNe$;4C!csus(E&8hOF|d%nQvphtuFObX_&(JFK9JG}QL3$FQHk"
    "NzLI%`A~m|57?Xt%R{;L?J7T@vFzkJEC`x?+)y$;Yq_mDzfN6q`bmsDAL%)b2BK<m{zDb+0%)#mkL?@#LGxmsd2N}<<Vw=orP)ie"
    "&w!|Tft=dZ&MpI;LI#f<<sr$CDPKO?4L!ei15*xkf8KbG%)~y!t0LZ0h*uq8)^4+?I_KeKd0^A9MX-UcoRZQ^%87tZsNMDDD~0px"
    "GIcKN226-9`e7rvjn_wQ>AqjF<m=$%=?rC5*3Qd2?RkyO3XwsDwyXTcWshaQ4%+iw>wWVScITSIgmotM(ymd<o$JN-I)ZK%nWGC4"
    "ERHn5OXGHxuVcn~mi=!@eTvh|i5ZlSZl3u&@PSxPMG;bfjWo-AMW;bt;<b#jc3p-n^TQdk89SpKi!TRX1f1^myd7*k0Kv=D5{ni1"
    "2(@K<S+$en<<93O4%J=9Jb&sp{uOO9w$G-CmO!kkp7-k*b_9b29<`otS?%SGvwE3MNqf7__pzCKH!vq1`t{{`G5@evjtRJY^ci`&"
    "(uJdGmKcZx4^eY*CW{?IzdwZ-eO4W;O5JX<U(KvY%muB4Skg=H?Qn*T;L&9pT8UKN|LQ8e-FM4%<c7ZWCD&}naNDl(CuQb;CjU8j"
    "y8S%f{_vsns=FM@BvKxF4IFv`Km|wC^J@werd};jjvdT6e=-J+s%c%uaURa}1^pa?qiXq7Y$ZaN*j8R8o)LQBti}`WZU!QI25+;s"
    "5%c<qixn~REH^vzZP%RfnnXsDJJE)v9jG{M_jSy(cV8>0kI2HeQJs^BK6k<&@y}phEo=Pa;4$WtyB+bivN~VLTPIsW(W8cj@Qw50"
    "Ic#!x6dxd2>T%J+7BpSoAbVVPUn)v?;cko3x_sdW`ZUcFcGlmc*>7A(rdpgm*5k+*QH8;_JDAsZ%Y4)&{cyg6Y#d)cHORgxk_Q}%"
    "_st@Ayk&w8nc0VQ7msgOl?!4;&}-X=IlqaS!%?ONhg2^N*u~2NG~EO%z~ofUh#)p$x9a<5MUyQA!pj~AZX62IZddtD%$!dDj=H2{"
    "ACQ-dcvBzo5?`-K^+E447}8vKLLw>%%@$N+Oml1l`F5EfG+#Q&={y9)>?-}VkEQbzfvVxPUKTIs%h~14IOg!~>)0>EB*&?f-FB7w"
    "xBo-9A0H=Iisoeca`AKDLmOc=(QQi}hdq*@B|^m6m&@^RHp}F+TjiH!nv*Q#BQn2(kbD&-;}xuq{Me~_hoV*sWpsY+s;x}omA#tv"
    "#&cfR*na+>^2@a*rtT6VzggD6sZ~I+>M8%&KB_GBG<#)gAZAxJr5^US)A}GkeW>s4EBkC1uO(po<~sQ3s#U|qPj(pCmObReyccKc"
    "K7qyn`_Y<g07${G<LlJevyC$8a}`r4SCjZLmh+k>mR^|g@qqjQ=UcO<5~pJPnogx>wC0GAQ<S%p>AT2$eM+fo#;8shCcokw%5UWs"
    "(yA&*i<+u6O0Pp=N&+CB&|GL>2T;#;fiU}incw~Ys%8O6r)pUI!WX>D0)Sm=+I{KldalHlp&e?sc$=!9`B3i&Mh)BCuJUg_zi`vg"
    "`=N|5OmH0VR??@9ZBQ>hI{>YdUs5C|jA*GygKM=Qr<c>)&;PZ5DgB{>(G%@Gaw=agSrquR^N2Ha=rnvQFFgPk27Ze`L=1<;m7v%$"
    "nd^_VsDarV7`QK=T0tMX)Zcgd{t5|H1z|lj#)KLiI-A7helw<e^l5<cx6Aw)W%N3GY97m8QaQ${Z9XtONzZCHK@3{`X7}}Y>}u+^"
    "^VurS6XthpD}eEf|CeBvGvh>9;f2ieJ#K4`1K`z<!yi_b!Ee>8!2qq2j*2HGYH8$6+jg1XeP*jmD1Yjc27j>!!?`<BX)9K(pQ+3@"
    "7$lJomMIQcIo@Q8(}tG%KC$nYDgWkL-CO1Bl0kY`Ts+$Jd-p4Njjdlc^uIe`ekp51=7(-ZvPIHXcwf%={BdFAr&1<L2f9ILo`+8M"
    "p*?Geo`j_J(!KoM-e;D&3^QvdHG|y=(rS_Jmiek?UYhXTM+n}>>#;U8drD<htwvr3-xn+f8eoH5o%XS0+r#e6u7pV6+f}|{CLczZ"
    "dpmFP;Z@AE?3dQ?{iQv&zK<2$)a-B-y&UXA#ivA^yo6l~2+Mp;bBb9M2QQN1p@#Yb9X#I|326gp_MEd{DoWbjo;3v~4kfssBGBDS"
    "&uhEPPh>{Rr&m2+9Z|vjHq7z0i8E{|uQ8StqWHgu`*?3VD$gNgD%u)YfZe{c{M9pAg|BhLycAvVig&0k4dXaQu+it4y1|_0n7ea?"
    "L0Gmhdt9#Kb=`I|f3-Q=ncx{mbCwK3QSFJdwIyctJ;EAbnKoqUAzeZ10_>;oFVL*p!N=`~`4!FDGDnZ~B-7}TWXn+^P{H|!|E1zK"
    "J#@Wh>~-`)IIe%BJ`iN^&ga}Av-opkNw$DIpt`rk8%WQwhRA5N_K;i*0v7$Jy6vhd8;_y0(FZPTSX-s??JB=E)39KMo#V|SN!Bzz"
    "sDjN(PlefnD!N|v8FrSmO}ArzIJBpSUJv#TY?evk-yHuq=vlRw>rU)eckn|lBk1tci|#e>>B;^(ZPx_F%}Xz>D|y#BV%x9sLuRs!"
    "sb-|Z5S6p>sJI7zyzOJknpnn6h+86a=g~vGG|##<;v*a!UUhF*`5hRQ|4tRKEs5}-3g`s*kO3^odz<Itz>`lK=O$}5nx62C$DTk<"
    "#W_e@V4kk_Z~hge*{r=AJ0Lveng#gemG!}{7)pJm$r(SKItH<|ezI1FFD@^&EtdUjk*Zdnx&a2rXAJuXEx1FIyHHQ-@;ebr?VM6C"
    "mAw%gA6Cof1S*?(`^J8KM&}Ytxb%s*)t3jW#-}93Mr<p1k1&_`A`H~8P|l;waXyo?we;T{H;UVb`Nie<Py6?yi*o|Es*wWbfHloT"
    "ayGI^n3|3t<-T8qQd4lU2*6Ij0iYk-P4>l0d*)hE&65X`gdZ!Q^G8Nk5ofioYa#Wj^O9Ecjw2(tK4T8gmazkw-y~yhtI33LPsp%D"
    "7Vj3aW{A~p-BBE4$$=HysKl0-IYMI<)Te{ILF?S@D!+KC_KTwsdc^?3In=ktKedE#Io|@-=cig51h&n8E}OC%EjhJWAAXaa<U6bU"
    "a9I<}Dquq9J(iRRU!7ESc#pYybbHfK%tEK<Wf|NS6V3&=0pYZ3o5;SY%%x*;1LEFbng5(>S;W#qY1)?f@*5oYO2dmzJM(XCz}aQD"
    ";-KeAO4=o|Ki;)9ulv9I(O9ErN8{l0_X9VMIx~1HY+Ey!qg(tvNSjxlkJE<rQCpp-*)H>QFH>AL+<X`{rjkLwlJSSr<1MRL0XpxH"
    "epM(ga4q5a(#iE63AU@!NZ;Y5{1dWREpwA#94(fSZ8>gIl>4wp4{$UhKj$R<#zPjULcT_aWD0$gdmCkEUlhO5&()V@R|j;MNcm_^"
    "k%%yCUr?+%Jg$&b&0Yqwuhw;Zq7atA<*}Nxx2t@PPp0Lpq4Qm&U?4`aW;kl*>{WF#2Vc!$A9hpIKs|DDAb(C)>Yc2)+;*AzYyVA("
    "eqRJwV0Rf_xOFJS(}+`AVCc6)vhKa|$ZRhqZiybmGN<m@Am`jZO!(NJBWCq-pVp(4Q{USAQL3=%nn+GaYXnCnE`_DfTPZ~lx$_he"
    ";77>Wo&Ws1n7@|tqeVF4ljh5O+^7};AHSab5)Lwsw44$(v^nnPy8;WOn=fA;5YF7L@@q1&lKG623@q1%{pF@)$(vP3Q?}h;R#p46"
    "w6qM1IQ&to#P;f+ns>HRtaANZ|0krA<fkH5D~_o|JIL(>;jrk?#_?6KoXnZ)W5@Ze?xl^5yy<h-mmfceWVoW{#di9zh(<3!*;x4W"
    "+R0mY(uwuxY(uRL1%B)il8*Yb>F(aE%<a?sQA?DVJE=r2h9t$J56-l?4d|qA;ypPsrx(A_3Z>-9FZ1l#0ScvDYu}#C-&kH)7iIug"
    "!u)F!OBif5nDDx~Cfy9L0hc0>qWNCMJocNNL^DIe>j>vAGUs1seq6V|9EMc1C*{N|@fjY=8|-D>E1QlL&S%(lTvCYl>ezlr4?Vm!"
    "g#9YvyD}I3JI6eEc!wC`;0LKTrf{vbIEbaUD9TFN!F;e1G^s|OPxrrf*8t2i<*)Q-((a0Y2x)X?IUM$mR^Z-!0*jUBgp+Bln7ab}"
    "Va;U)Sy`D{S#4MO!9Rf+zo=5u=lafG>M4e29|mAaeb+>!Ydp1cUxQcq_Ni{BD*5o-v@2zl<7>^fV#Yht@=6y^E3LLpLxcen0Hbz*"
    "*AyA>3aDvVFjn3Cns<CO6;3-HQ!n>^$;^8>5Vw=#iX*oC>a=ykm5xg_qw^h)ecJZ6f!L*CM4+NKbh5f~?KB2s`Px6#<OGdt_SIBs"
    "cL!@OA8OHunZOSwIa^;6Ex|h)04Zb_>}=cHrKjR{m4EA*S6=^cLY*AU|L942YWAhwnnAJ32aZx}&B73V4c%K3`#E_;ck#dcivMZB"
    "KMVbRO=gnmZ{AOO=A7Qv)w7&VG{MA!jUWyoB?aFmZx*7~wIbWC@(ce-fhh;>oxtOqO|?XTju+_I8B;f#j=Sx;vKD!c$Z26OZO%15"
    "LV(>?F29S+9IVQpNyB&o%vjp%u!pJ8vbt&2VUy9D@}E&l?kmkJYur&@{*(U0?{<|>UN~9C;ME-V$=kQ+uw7ryLdT-8kwC8`Y<>xH"
    "rQS=Zr5x)URtsF)J@y+iSkzyj1%~ZW=_qS*ua|Rx=a@PFcB|;qbU>qnjUW!@2I6=jRcjHj{Vu;^$!cTaN!}$@1uwbV%Z}#jYa-UY"
    "XdZFgz~=D2fRK~e1wM#VW%Z&C{JTxdC4c%7`u&|3><i?TSF1e<ijPcwx3&3kNNPOl@f?}o4oJwCQ4%`b4jSrUv|Z(QjpZ04n;7aV"
    "va2jGE}NlrvTlix#gAi4<g?u7V^Sz5KUuZa$*$|Kumdvn2Qf9rGmy3MLde*m!HewL>)>C!Zm<#yx^d0};1Ab4)MW<ioMo+Qy<x{N"
    ">rX7REP26Or3mo&a~yX0N4@{N0eDxB*BgS8)&Q~NIN8rUma-zjP|yTc+f}{^GZi`2zw^q_&ZTw^HZr^SR?PHXIBdihhRg<eIW_le"
    "ykSsgeKlty(rj0$e_Owa`%_S#kEDD>U&GgjQhdP3$9~2xJcCVH-j22zUmZTK_Q!E==L_m}{+%zsUaw<GUTUO3g5FD=NyiEc9NmG*"
    "8LWxvTre$eXEy-wGCuZ$?2cvqp{3Q41L4iqs#+eF5|23`m}D;!awD)E>X^0rc4!w$NQQP|@6GYb=CPgSL;PlCS5IagDO;_6__Vrx"
    "*bY$InjSewkig>66t~F+T1}-#FIlmS>~Q(^vZQ{lf@$>^E5~sKJ23ds=M*iU^=RjNok}j&u-;|X(ehd2%V|^?XgkaGjO`x#Ix=Lj"
    "%6V1({Qa7F`zh3i{GI2&Ft`S_SIL88L;4vYbn>;5FOQx|X&3tIuZG#KH7cR5gB_Fv`saZZlbZEoE%9}%-gI2-WX)-+&bn^4RkAo#"
    "Jlcl-`n9mNRgdKr5##Mj6(_+hj}*!ag3?VSoeu0n#^fRX<Fw>+2+zl@spRgv{IL{OCo%CTUEkA^>@)5<2X>`q4?)pcCRnYjjSL_I"
    "Rp>H*I5tN%WK(YG<!8gWOjvp00Dvd=PO2wVL545FO@6b^E)F+B@8f0JQqg(kpOU)oLrL1XjWF%&E^lRN3-UqROgeigxMXKHi8(4U"
    "5@HQ%jzey<gH6QfB=rxOk4DJpX<PJfzpk&mlx6wFkE6ZTvF4#=LQu3M*?b8AOhjX-9$zcQ1^8i_*LB;Maa{`bZCCkKeLa?-{&(1E"
    "6&+QLBL4(p%{?qOOUgNVzj8Zm-VB<gbF!Mv4a@u^ZC5FOd&?Y^^~&2TF(Q0a<@an_A|`^Qm23`_gC{*|WC60c9d$mZK9KMB0^^&="
    "j7uh|0wZqn?^PVWAJ;pbGWE{I%5q5E5%W>hD)kC>IisX^I&fNB+ivCWIzBEnmPyzRdPNaP8tgCH<qqD`VFnUG#407&Clks3)0TXf"
    "$&P}-;<u~Rzti~BmZ%BS(~wkm9s7k88ObkD^?ii|2SB3?cIR9+DT<x9Ybam0oh@lUd=anJS?HZ|C5nMyW_xe8R)y#FLgF&{cr42j"
    "Dt?bf<YMQsNmhG!Z2w`E@9^uvTFE3;x$pZBase^{=j3^C-yll13q}+Q5xFS*epymDrbDQc-)-BB)qdsJVlU<!;NsAr3v`^D;=<0A"
    "9bMUwX}Eks(Y5Ra>2qNCW369ioC>^XJIOzw)bVf4*oT@Z_e^%Mj&4WR^^jC}lJ^{zdO}-w<Fh!Ir>j(hu>mjJk>9Wr$&H_%=&~&a"
    "W9hzi!@O-VcY0wq{*1SW?;VWS-Xvwd=gAI-*}j7V3ei-+<QUskK9zJE*Z`t@=1dy&+Oh^fi#AKSpHW@uBUCL!6dOF|V+O4c8RrII"
    "02br!%cO5ia><v?%pV1*)GPL$ZN_*3{k#-qEQtkdGfpaW)n@;EYNev&y`D2bj-6{YVf7cuyo;X~z)QCiu(lZ7cB`ZXl$Jl2vUQCw"
    "e!`_6n<=|GPoqkube9?XRlb>DYYTkxWFeuew5R^<#dEA6<v?cDs5&f-CauH?-_AH+w!8{vQhm3}e1@WH8fOMw5xj|c9PEe5j6)6V"
    "hu{@-t0P9Lqohe*QMyJ@d%k*umh0ZF^5skYKCBqr1bW!$L!Q;abXmqK!d7jZD(6U%M{rIOh<<ukqx~5VDo)(4QvO<sy+m55TpZr$"
    "+b_(s8KKp8OsXwbyKvS%d*FxvcqvAxto}oFHz=Qu&$vw2>YEcSsIT#sAQ5sjgH!$_8BW?A^huRXsyd&H#K7rSb5SDgdG1z8|8A0y"
    "r-OLPjb21Zi1OPkBixRBaS*)?s$h>xbs^Hz6dj(Cgx(%9;SMabO#CLx)nYsBB2`h)A`b6pE;zy4Q{tcpcArYc`K@Z#L6pds+OkFf"
    "Z`j_(r9XzEMaAcGHs`vhTECWg*|K&&!h~;<3XD=|ggM4cE1dClme$up_DO>2-7=p_UQ>*1swP&OcRr^Ez2bf+i^H%?;SeyP?Ubw&"
    "d4lc0Y&m6g3vZyO?a{2Cew~#vA(E<vNnMr~{C_pA7W57ilsj(#pM*-Y-qVOjNl74`6QNCK+(xrcCoBC^^-X+rjkl)P4KN_B1VYwN"
    "_8yLiT*9NS<sZBGBJF9SmC2pw++E19O8G1Ku5<4-=Ue(b>{dJ+nK((4AW85CaK+9}^%<0zoO3FxO|(AMip{$YHvQ&)mrFbg$nGwB"
    "S?lC_-FATT#YtLc<G;||hxq1=FgX3O`GlxOAk`#ChtcgS=Qs4Fd?CLn2+Z+vwZmH>=vYDT{ML?1yswSF7r@fO;LPBTQsExZ4Fv?+"
    "c9lOf|8k7;>fR-v;nC&GRy(zRFK}R7w{G?mJ=t=wVyT%#Y70}!nP29tyJ!}E<JieLc|PlNdfAsr8d|N*+ZGmrR#)Utm9aL*?a+*s"
    "()WB$4=wb0O8s{MApEpAn^~qRj(^zFHwy3wo|#3<{e<^>J?cG>+S1`Vy{9L#iEEuS^6q3eTAsd<ul&yqX0+o?{ce?3oq{Vb`<Iva"
    ")Oto}ENR~BSE2xv?kFPV5fmEB+bIcF`8Jss*hDG^pl%3uu0lHA+svSsd<vU79Irq!If_85;z&O0wByxMay#*nJ{!KYM!ri_gP!Am"
    "heniofRW?SLw=dd1<euF_;p&D|FL~RaihywOvSBttF-*@h7V75B0cDeW*k10s3Q{PGHuV`u!9S8yK9;71ntyx=V;HgmVItf#(8``"
    "?<z~>SN%;jHr`Y2SL~WlvhMPp<Qd9VM*A~#5$~LugAJ$AYXh+F#+TsozY4a^YZfDp7p|vK__RM|uHI#sSBsJy!f&}AP7{nGMZ1_Z"
    "uTPKnk>{=zGyO4@gZBV3yH-QqwB={9Ms@aWQdZhUJ>-`pLoSV;SOR$BWjd8LZDJX_mTwCaze``or1fvoiyRP4o)5N1s2ukUZL=JY"
    "wR1j8ryGZgZv(0Vzz2Ib*&4@gnO|qosTLz>h>UM31nm5%Qlewbv~~d{iqFj(15mOhm%MOOFxz?*4EWKEbGOPDWZE*Xh=jya?1Ho`"
    "qn0K4FzGCB`T`k~1^7?*vY3U$gmcLOK47bDXA!IXCX1Kk%TmX>^71RP&;J1vSN2iJoNqR7J~d+`{RG?B$*YIreD;;pBgF0Z|7kHa"
    ";U9Q`W8VqD!xp|_Q)ElSRwzA1E|vOVAXSHHVMuuSzn&rj;MdrVm8uNzH!DBHDNn+2OJevDZ%fvhNV--;<9#OyAE~*w;$)Vh0QQ88"
    "4CgJoAak~_vp~E=oP8z?MtL<yM@M$#>kXtW&W&S)ZE2-@!=Xz$!2MBz_~oCUX}iZ-{oC-*F;KwM=Hk}|Zs&5rcmn3>*MA(7<db`0"
    "c?v9S4hIP|2Mdnvx%_<CE-!p93%JuPNOY<XOpsqUE1LUwLV-AAIfy#3*tKc}_gItw960wsx2yb~MNRn^)ct_8s~D2EHAd8UwItQ|"
    "&YoTOs?)R#>V&&O3Im6AH6K2^aUV|9uOM3tnCT?vLKU?QBnC{?LYjN}pO1B=(FB};fogmF?cKy#uI_LV|8)Dxe&jE$rvc2V<<Z~^"
    "ak(YMWY^_wllKi{13pils@ll}aX&3VX$^X<`H5X)89$@_m3*^_Bw3JNa-1XSEPThM#0tn8{Lb~svs9~Sm-XdAHtMP}cp1F~|JRQ&"
    "MNye*waZYoI4}mue6)Llu)N+Q!l3eNRjh#30EE2Sn=>_dj{2%@6}PM8zk&}0capL04eFF|G?!K|5A5}Cf+Z_4%_k|gGU&W?=3x7>"
    "c*B;j-73Fo8PUYcSXhIlQf>0swH}uwaH-?e`bWJ6FgjnNUXqQ66PFG(Y_A($wnZ{m`NRGkkuFUQDRxuK<2s%MvLLnJy1g8{x(Ch~"
    "&+Ft!Ln@Tuf@QEBatJpB<k>EB{dLDa?_+o$35IExszJb(qbx-`sEk^M?=c@Qd4>)whXvEi3~f*v$M($pnk3igI=}w0bbd9JX?9qj"
    "$MvRa-_QeB(&)E9i-xpu<P#X6OLI{4`(owYDxcujs$=Fz_DC~d9jM1HH9!RZ%VUBt6q+Qq5HAV)*U;V+n3mNt0Li>LZJ*`WBa^H!"
    "3YMQY+zKPlSF`v+%DtOFkf(Un%^LvQprfP?jn`>I%ft7d&dY9B`Q62@nUWE-5+9c>wt&Zn-!Xy#XHXAGF~H<~vm&rglj@D0N`v}>"
    "{En@01*`m#$dbmYU3%Tec`f#EE^<B`DuL$^Vfe_(`i`Om^vPouRbH}oxeAi)l7IX`%%-W3ZiM9?>{w-8i-)3}+OVmn10Ww>h_M%D"
    "gg`;mv(qN<oW4{7&bVFWqkd)CFg36&Ka1!=92lzo)#o{yZV%pR+aXknMFE0ygN!*cZJ{Ev<95eC{^IhYu~pr3I0^i#D8kW_Q|S;S"
    "LQDiPJr)Ky%d7O<bgczOk2hY*v(>zPmhh<xEhSbmNqLj4LZ`l^_6<ixh0=Sj%c|y#ZdcFdxGTsHc00%v@F%k8&QngWuHS9EqViJl"
    "rW@5Y%Lz9o4G-DS(C#lQW2BO_7a3$}UD)kuxL}ORr_E>k&c3RgrC+$k>;2ft4}&HXH{YlWMsto8ENJz5wyP)xXI10kv=C7f={4I7"
    "v|Hsjy0pw5rwTGftrgIiCYw;@Q+;gSe@!p@DeHs=s0y0c!^Cxy^4IdlF>YVkFUdp|a)iM{1!1RZ!FGuRLjJfN0MW5gUAyJ*b;NkD"
    "Ub9mfn>8U<%eE#Pf6@iT+ju3`_{qahx#m@0H*cP#mJrI@$pa2Fy+!}34#`j}O*<$D_Qy^kk5#@oJY!iYya&U^2)T8*W_-^zf^Qq-"
    "&}95lBJKs4Ukd|rZqNJGS81GMY+u>WQZCwyV-NDRlzd8yOBjhna_~bq?v=AzcwdKP<{n;SE^~%Qmf#FS+Tya$QSP6LoEk`l;F-z="
    "ZJKAEy8Bq-xnYyf^p-C_-oU>aCaoEI8-gIB8USxs`52~9wmj>NTc_?iE_89KTHZvN*0f1cIElz*=M<>sNzix|#`*&$=-V&zOUu`4"
    "_wyI!A&B?kT=wC9S_ljqLE~VK#i4?!eK<um;6$o9glf?BzMGFzH1ZeA0zpRSKz?SsXYrCp0%O2Eb@JvL$cs+3H$Wt63G^6rUs0u%"
    "Z8o>(@+)F`>pvCX)EB`4aqAOR;V1x2xCYz1^7GTW6m`U8%yVX_s5YZyas$z3mGq1LOS&+hLqpLF7rUfnYeY>W#+nG5@r9{(Jp|G?"
    "te=Pk)nFw*NA3Q0kt}_RGWL|h$xM6)53fvag#}+^Qer;<m+zFjk~qTk$H7)RPkE#xYH*;m>~@v(S0BUr^eI_UP~J|lczeHe@r9jJ"
    "jeN7ic_wGP_0NihQ>$@tzF<7LZ&zu5T`P+tFXWF@qt{Ii!9}pWX0-{PL6D39U1+;QdL<M4RmRb#9gNA*+O2YaPCr6_@GxIDOfo^g"
    "y%?MPP*Ay>YC*FBAK+<h&K_gvoErE<0NS_4XZtL_470BN0P}2H_R{1Cc9wz%#|N4L@Pygb_<D=>p2xTep!J{kI&IM5d%?<gt6X0%"
    "j#Vr*Hc20+6^nFDg0BOMWh!V%)`1?(b=_-Y;hZW}G`0YVdffJDY#-)VD~XD*C{LmUk@K96j}dBWc!s+Cpk{DDu>;&IClF3GeZQxQ"
    "a9NY<b0@;eD!(g?V|A`JJwyD?t*5H|N3u!Q`rLqWoWKt)Gu|UQ+;6`}@FoBb42|D)Fr@T3JsQZKRg)kN0O}9x30iIN+7GpFJ5O_^"
    "3@kcqmIsME+EpfJjQTmRH|zjr{@wbL;q6|#<H*5MIG`fK?!keu?G7H)L$q5<8qZTnZQ2(Qik@-X{7a(gTix39&Bufztnv8vf-l|-"
    "z5~nywWiKeUR`@d0skXF1m*Wfl7khoH5rEl2Zeg}%lxj&b6bL9Pu8Z`9`Z_Zf~vTmyTe0w(`lg@-C%=!sRZ6@k1cbOM7ZHLmE~_d"
    "ehpz)#hszs{KkZ3UA*e;)YYR5(_^fc`T-iQGwJp&<jXIKUzw9DZJ*{7E?Kj_+8;LUDc(b5{^*FDEb#+ACb$sd8o+3s-$kPy$FIo{"
    "De<I5M~2-pU;m=TQqW|0pTaLq0MotUR>nr_A^@`!>^94i4Zt*Q>G3BO_*CLn?y7A!+RwLqjg%5&@71Z2x-QVeOH+tMKR7uPGRAkf"
    "6xB$hgnFo=A1)iYhy_Q#`*xLImt?W<Z<z%Y4C7W#_)yQUX(ma`v2N{zBysBrOBA?z3GhHP$=u@7yz>(#%H+Ro#Y-E5RH-lFK^F8X"
    "o?rv;=Ht(+sgwZRFI@|62eDwb{Pc2$8@9M>Do@`?$!d}24BPQ_*)BzdnbAQ=$ISFuZfbFos)<7Rv^F)__u1a5N3Z!~YmUk)zZNFH"
    "zV&;u%IzI;XVVUyMb1{2DBj?#hyG}faLK0B(m01+3GeM7*t5O&DW9RxBIRC=_C5k=3o3q+Z3MI@dG`sYkV9)6jpfg=gwzzBDI6Zo"
    "CagMcSNZXnV#d_ZoA?M9v$yRb)XZ18pu@uUqo@nn#KCzG$Pyz^lIOffuspwI80B{z>+h$YoVIl-1h$1UvOQN%OyV5@3CqJdU&^m{"
    "af&+Vlyq7tIJNAlJ3Wc?S+FfVISs2~u~V`pt-X#6h}C(=l3OiK58UfM_EA~Zxhe3DGKm`9jl2VY<rm7TK-Nyg1DHp0ZYM$dg0S$b"
    "k|@c#5BY2Gwl2kOoj7dw(7cvYTLsPR!0jrZ&tI?|2h0*GF_<5uETVAvj^*y^A<yUTkiM6#!FlbcCXHG%D%vpZW!pZ>rzd1dhM)CT"
    "JyoYp!QLf-quTjquS$G9%VdIZ2L0z*AFHR)*$<L++e+}KZ`LDRtLsOS=N+g<h*H{7`Mi+*t|&WOim*0xlY-GcL;H!>(b%Rjuoq&x"
    "WqxyOr_*tuM!h2DwG_n0t3ZZo#Um=hX4g&ZI)INhlfnsMjGF^#RqIyPV!O((ylA|<)u2!v3QkYXU<XB2%1MRScsnrj>{<{g$a|gD"
    "ac!RHZTC_jdUrzq{HR~{4_+>g@F>d|6_VpOzj*-g_&p!aAn3^|q9Q3)2J7&DwcgYY*#7K$XY>Sl{TOClQtw`f%f{6Yc@-0!v#9e!"
    "cswmXo>ddEi^K+b(nj1OUbQ=@9%1`5t^6%xc1ITg+lMo?ZOOR2ONhtjIj0Cx*Y_dcuTCim-U$!AAwlmEKu)_aQ@^=2VupTFwJV5i"
    "{{T$*y45L<`br;$GwrFTCPSRH`n(Q?K@V(qI?Ff-ce`c&2z@%YW`}hqMUn}R_k@<G8LaQW4q7;bPawX5IB(}D>HbH!>mXec8V}yK"
    "tNhy659^p#@vdd+t-c<i|9S{NJWylb^7D^3#`zrTqlOfdLwb{K4-Vi&)t#Sos(-G^t(Kl$C4Vbp3Oh-PH9(5$<i4Z^PRs4cG7LCq"
    "a28b(fiITh@#)fbmETl}88f}<l`0i|cHLdozTeBLQ?_05<+G&YAr{YTW*(>;a1{KA3Ipz_O#KGe{4i=?YC-s(-Ift(>SaF$D8GB{"
    "`&`lX^62F}IQJxhonhV`>|_^v-tp!73z?h_&3@ttz+e)-zEnp730>+jjAeayRT%;7wif~sc$bf&Uy`i+D*7dCm-!*tLtd_pV^H3|"
    "I!TyrsJ@M#;dVgwRu_$LVB=hb&vfMqB8l560WDhIvR&pIU9*n;@)93%#-WziQ_DRR5J*t))*cNdMu@@->#1bPTSp20^aDbD(oVNL"
    ")n8;@E&rEA7~NNw8ZiVT&!~|*8|xh2$!nLUQud<)aGfXHVZ_|C^E6M{uku3~S(d4>!g3M0w7hsU$!Z0V0Q-WLU%V}Qd^Ao8Jd9@0"
    "S5<ggixeow_LY6Mb!%>wFB;j&k@u6I%o+{eDKi78YG47l02Gi}z_>L#04{V($vR5S`&B**PK{mwcx3+a9tE-~a<B)o@+p#G-);}B"
    "@pu3TFR&u`0ka~fr}L$AyUH)hK#ln_PQ<fs0n2x(7M4kwd~{H(5^xabM>Tt$?cgA#9L^%BkdT}Jh})3)8NDA`@Q$30CW2Iu6f91F"
    "kS>EGU}|?huX;I&#de>?tC*^b$?)odgIu?*GWBy{kHQ%>;=D0vd}LjGrKozWlhnKtcNk0o5O6d6&HnK9=SlsgIFP%IW}gbjVF4(q"
    "aUA#!VZrILXINbvQu9I19DJotB+VyulWpmABj@$V_81M4&VD<815Br-Z12O>##O=v9Ld*d-Qlr|12U3x62l2H9pG2>ED{A4NOWqt"
    "ZEt_tSI#({kcJPbBDgL`+UolzZW*$Yu0fz&ndVI@R?no{0_dGoylYL-N#^Y`zs7Ix%lR>Dh!BR?XF7C{I1IcFW*nq|d0hHcC~OY6"
    "akMl6<p!a=J=TMFt9-fHUa5af7;oO!67|3N9u|5#(X6il2YAM5o9A&<E6z&&N;%^5EXSVRDjzm)OX)rGB#u{*XqAxBlvL?D!k32b"
    "nh0|~AiM9ahd}|vIKA6r4`r+M$tvj^!`JNrX5Vx?(G-tJ5HAa-Bivrq1EPk4o43XckK+DWC0VdS2LX(^9gzm{T%V_4T$fDKvpIC7"
    "<XsVIAe?-j;Q%{W`c!EQfMghZwMDl#v#hoZs|4F+KCw^ZSy0p0)L6l~P*@`I?CkA_Sn#ScUTx4__vTR#fZV5dO+1<t6WfQ0p9-fL"
    "07mvEMK+?=POWnq5+ThRQmAREqrt+9hPLH84A2B3GSK6YaNd6ZU(!s=Q!Xa0fmPnnj4PZ~pK29d*kn-zn2Y6%^HsX=&DXr)qUGwx"
    "rNHg(TBiMyX7lpbNldaP;BKma;~d7arXw8_Vfs8Ttr^dk2yr|RLo%MXr(KR2?|b`Qe)nbPVY8eT47Z)}pp(>Vu~BL&!Bw?o29yOe"
    "|6w6c2{rdG>@%s^&KZtvyUZ_UuE-2^Er;659Fv}lhS<E9W4_g^9_8!RH1xVd4OZB44&HSIAjw6{*{xFk_~R7Ko@|6A=-R?C<IoF&"
    "1m@u4R2?TyRRpUg=WOqBUj23|ciU#twjuLp^>EoOMGnbB3GCXn2)yGVca1=NU?fOikCJ&gsth%}9GyK2?^scB2*b{Xg;hSQZ|e-P"
    "TQ9ROGJ2$QF_s+s;_v;=W@GeYU8QO|-|W`6{6AwQ6p?av<-hhTW}Qpw-#eUjy*~JX&26CaDP#rYR3_-Q+$SJp5~7LEExtoBU2lvz"
    "yik$tGG7z>>dDop2p(=2uKnU*j_OPtjMTx)sWSes0nAyjekEdp?OXFXi7{i>k)MyB)kC<lEk^H2CWM&UZ~5x8WyA5U3a4fcSv7<}"
    ";%}>!LB&F%PdMl8Ez!3An?JX==oHI)2*GaU?r%1&0yZ)kg*i^|1Xsz-E(E2GRj&b`sn(iqRp55=i21W&iw?1nOmqi@2k;55hg5Q{"
    "M#zI4b56a?o}mqZVsz;^W{A=2l!rr^?A6+?^226HaL$u+W&pF$?e4MBDOTIs<-;(aYzOX83a+%%0A%y#(0!2YILmgK-$+jLDjQA*"
    ";p0cw)Jt^acG#?NdY2jN6p9}Ww!~++f=$eDLjq2$nku|uze@aj0jP2~WG_BBXpBP49d(s~ascFkDHO~;bP;_c`PPbtVZttPw#yy2"
    "t9)T*DdfEb6M)y+0+;tReFRMk+1K`8K9g1LL4R^9&Ibd3%Ti%>>iy2UWy;rNV3+|rCN<NS_m1s&N+^-ggbH03$rEhGr%^B2Sr$S~"
    "*J5f)q291vCI2;hQ!xvyJtg3U6!?%w?u%5$2e4b6W7Vonr-j4-Ndi{~{p<7cxjnbfQvXft$swU`*Bv2|l&e)G(N_(jCu)ys#RU4M"
    "3kO^4;J2^E;<<W<XUyAkSwBairH^@qd%3O6!e=e{w4lIwh*fqz9Ka>$yrAhjDgGDhDScP!=e4(fyUg$VFL~5p%a3sGdC$T&=0EB5"
    "=;iqnL7oaGIY!kwplkxBE{>7H$zfU<e0IyUPf}h0kgt5a@cui<B?`eZJtguYIiOyT@eaGLQfG6}BzP+u$`O{+dD)e-a`?4NXMOAQ"
    "*~WFiFxB)`>f<;k2|8+u)T&0PY&UrI4IV$=J|Ju9d3m4SI^4Uq9M0QMAJZvDwkA9oZsd9C+J>fEB70~{K+u9dH4VRX&+U_B9_`?Y"
    "r#9*}?usUy^6$>XUDlvrz?f-YKf$e%t8^=71KqVp`*Mm#CN=<)>zXxC_{eEO=he>bDxaZ%ROURcOSC_Tj=;SmJ14!AJqZs`b1zDd"
    "^`hTy@zEy6umo+Eso`#w_&rOoS@vo5hM0oV;f`4aD^&|`y^=0KvU9}>=0O3SFDQy5z%?&BcI!6Bgs&@W&3ypK$WJQn<iiq-dMm0e"
    "oRKI(X0)H?Jg2%7d=gW&`huJ<eGbB|DKme@b;Z(LBnDql##4LJI}s#dsKC!*2h-_UdKIX1-fbv9K|uJYWk(CndshHvmEUyf(#cT2"
    "<nH5Uqh&tFEMVgiVF4C?iaA*aDfSnq@yH(2mtCE_m$m=@-Y(Prw*HfsBbl+^Z!kd>&7CKQub#=8najU}lj+TZY7CQKMvKWg4x(J$"
    "e*aH})i&m67XeHOt~oi|B>4_DBg2p9>EV8_GBi)>1!|r;=~_|t=%pFC=iITs{LQdMZ%q05p;`)Y2$NsZ35IqDHIZac*38th0V9IK"
    "dF0zGVBiy&l&`oWe|k;-?q$tnLs&mGI`x-*vZHs+CQn^cr;gA1aXQ+hrKBCMQ0k)b6ZC(lDwIDru{HGv1tvl|RUB<n(Uu&p`erLZ"
    "YjB;T5#KJGW#^F@_;60kX^ngJojG3qe7>%AWir6=?1N4?1L%*eLV5I3W3tITh{S3re|{&gDCh`5mMl$mxfAZQe`P<yq@7d;Gp;iS"
    "W_1)mt4A?^c0jRpJ*B}N1hg0(3%(<0sRPw4Ljwek?JB=e<|<i3(pQ0Kq5zxusl0s$W=<9J3H?r+=I4nMUPw-TEp-NPTekkVg_!5B"
    "Ei2lTmmk~wY#-oJ?7F++11~D^3dxp9z*JWu0|H~S?pxV^I;AV|&fiSiRelYIwoCAKZs+lr0?(;Ax&=DBdb=-&u)?pdhJw12`Y}{1"
    "Zk)=RhKB6BRet4?B=m<r07>GT{HUZrKQ-x$WW)O_q(+iveb+e6NKWmrQV4I3;}Edb?JA#ug%6_~X>=;mM!2AqfrcS55vky;&PBkU"
    "qq7%|%iF_sTMd3ye<u8I_wp~2U7mi91IR%uVa{|ohO%Ko4Vj%5T!77Bf3d3jJVmpvFe~7x5Tkq8zOo<pzwG-?jtpdQF)&8iI(Emw"
    "xsK=Z$JG%56!Y5SJ_H~-fIk4uLp(~h+hwxP8Y_A%$;vk`t3i~Xv<4YRCgm54G4{yA56mm>c2o8PF0*CoIRK7?-t8v)JS*#x`9F0A"
    "BXJ!=ujyOqN)YPHfngi71fcAR$E)I(0!djP86GR^Yj~CIE34lw^NwkHA#&P4es@cAekP5+I381d91`{M9FESYsGz+s9Cm&DL7>|{"
    "%&%Q0+sO958X`K|dFb}$0N$Y_S&|(;HU#Or>SXjJv)Exi+195c;*U+@+f_a};jpMbz~1+y@j-pUwNwxkbtl_L#Y`1_eYHm8r(dgJ"
    ">opeOzL$5Wjmt8>0;4Ae*mXjTftiMxiw}~^67#$rBpl-ndUBo+>X*b#;J%66>}}0aa_^gK8IHvlWH|m2K37jt2{=WwQ~r<G49LAa"
    "7T-?&oK-M>dBk}(Q1ZBjQ%~B<-n-9#eF0Pdr2+nlmt~QmQkX_asdtU<tj1hk@xSr3m(d<XhjC3i6R-rl1p;qZ`Q80l_~jS&61(w="
    "`)Om3Ts6Xj)lD1TDzeWAKxs@e(JFY)r3HKt+R|}vSNU#BmR|{c@6fXE@j_%QVL39+m9$7DeWsy+8NejO0>gAPo$ol{mmLDIgMQa9"
    "ET7eC*_;-!?Ze^kIbp8^CP}M9?2@g3+GgG08+OFWziPe!iz6Vg-#BjDWqzB?febY$PQ6e$1ZCQwcl5-G5e(}50SzokzA*v&m+e?h"
    "C)LYLp;p#*m2Xbc&!6rb_{qwn=zUjLKWzwV2!iwGJcvGIHN`4MtqwGdnYvwCj!^7Q|MjVtlx2*iNWi1j73fcck6z`V#C7OQdj&S6"
    "HNp)TolQbqJ=*cQ>rJ8CP!@kJnIXE(&f*;UbsWterL54Bx8xShpO;5~9~F>ORQr{n@Q}Ih$2>5Vyn6P_{A%(_eG10onqaAYo=;ZI"
    "wV$=#m#*R`cn>H8Z^~+gII8h@wfYdi)#lCHRX#~cNtsEU?5o8_<0EH}V$s6{iZ^{nCxMY)<<JHegj6cb6Iw=ec7W#DuJY9v^k44F"
    "tfO1W@lKSppG+?{tp-gbUO4n1a_cd4kx6Zx1`s~ALTbxSQ!;-ZKYGI0W2W^AR3#QgQIa=swtGR=Aq8o~3yh>iVOR1~$rcd7s|US~"
    "EIXC79KYexU}lC`PTet;jelSc_jwMxWE4ZYr_2?K!7*GHwvKBzuOaV?R|;C+UCQP7^?s_x5!G02acWQ=IeY*y5Y6jdlTvf{RJ`D5"
    "IrBRh1a9q1w6h19%x71}W|dzwvngB4df^Yesa{IfIf<$D%Wxty>MsG0uNP36tYU}c8Lv0ysL<`6$bSAiAU+XOw6~s3S~&~>p6Bry"
    "{F`pVKoG*SMKqP+isFleqGs!1AV=_NH>;$NVp#d_FB|CTq!w?>?NFH!sI0iRh-JRfp*FT0h+Xk+!MZN;jw)Pbr&-P_zxwiOm#^wc"
    "=_8a90Ypvm{6t&m-#6&+?e*CtmmL-d;A<wn{wT`l)M?qFj6Xve)-f(lfx}8(ewl~$$?={#s57E9HlHD>WOQAc$B>3X@!bugH!t$t"
    "u2TOFmpUVqomC<89&YOdKk5L(3FIf{r<hgHdXAzOW4W6W9u9G3ivnfG{nBT^U`e)cgcPE0>}0aO)3662k*ARoIx-hI7aT#8T2$r1"
    "8P|Tv-w7An5A(alT<%NY(~;hJ9u4Z*g5r2Xk@9Xq{C#zEJ`Ro%`QP@V^9|D$X0>b*zt)b)q;G;fSuOK&0k#a3EKheBo;(kDQ2LxR"
    "LAoJl0R<mr=R#Y)$$<|=9+j!>RMK+FpXh^gUoQ;eI<<)=j0`CL3t{41kgi%zKqM$jAV;#Y9ml;CyC0M;jSz(GD=VL6p*BASj(@gz"
    "m*dgV`Ceka1YWbs2&&paDK)7lPC7%?+33Jp1wd7|I=0JvdY_6uOfZAafzlWy-er=HvU`I-S?wXW_y|Ef=QCCOmI<WDyCrPyZvJxm"
    "q!`26%j>%LOxJli{9U^VtEe0k)2sctqQw?^iXvGMGC-BrSY_t$ACCVoU`BQ25=6!~euE_vhYdW-ksxsfn3>aM00B*;pyUCDbM)qr"
    "hDyWBfqD1&^B-Ly@^UzmmI1GTR=pbaaJ7@nkCm5Ly>&@mrc}qyrg!q3d;!l;KW~Tr@@H6l*ea9n7eEp<;F%orb_2-x36`<U`6?&O"
    "nbngcQa>=i>eV_i=~8%XXY-<5)DJPq{>XxGqBAQX350naPT*xaDR&UK?$fToO5Eq)fX${$6X*ji;2C!<#yS5Ka<hV8aQ@N5P)k}v"
    "sdB5f@^IoF=jWboPvrEyj>56I;%af_+w-;T;NO0kUzCYUjUjPL4v<n_>W;R_iHY-Qd=x?uxglTA`yN`zj0u8;Jt;tGi|Dx<FUt8R"
    "!d=~EO4Bvct0_4VXp}0^Bc1%RpmgJ7<i}@u>?$DE+44{o=ODW7I{#TE{|w918e!JKk63ec1wOpfU<m~vZSn=c+{3aGU?WzT8=w8r"
    "f8_uZ_EvWjIV|(LT^7oc$^!himfPOf#>a<qPaX%}d@sKr&Ugu4`W&rYm%U)TFC~-He9rB|e6cKgBd@(f>{TJA*K$=l2bAa;`ekSy"
    "Nc1AV41kQCpTR*%&Fo3jRq6K~$jJ3`EHBRe!j+Oe7|ls$Ep0&LaKb#Y$fn+_m*vfO<Sbp#q(k-203Lw^0!ntvw9n-?E}OUa0qgMv"
    "k~$xdd|u;G8!QUM=FpFx28x^U+prS|aPQQXGkE#CrOf3st*R@}D7H7&G>%okleAkvdW8R_F<!vTO8j#OAA&&lQuWFj=(7DEz~gS2"
    "PmHyAC$ISK`z~d%)S)Qw;0do%C0U{z+DSUOb+T&Hl2;rmgGi-Fug}hZJD2m*Ffp8gkd<%Z7~Jffhm1))Nw!E>+p;!FW^rf|_Fhdc"
    "8UV5v38ooDw_D{Ze-*6w(qIVii_ya_kMkuN79L9F1yItw<Dy7Me<jh%967}RLr+lVx?QDxZQ0=;G3HBE(erKUqFBac2k^gR49N7V"
    "3Z>!AtXG2<UnU%n%|K_c6=}E3FKE_lRZOo3(&VI~SLOKusXx7C+j1QYuo9m_<A2+s1x!_dzw#8zB56yRO8)$}uXT?ZPmRu-O&GNS"
    "j0X<~=4-uQ|6C=q4X&AFGV|3hb-CyeHB|=Nu2O!_vZNADKF4{sTWx&e%5ejduXx+>ZL@hUYi7oje%gvkIMiuGS8Ooa?!(k?z_i#b"
    "(cY}52it6Bu;y$)W~F5KjZ=KWZGRpi0)(v9w>QnsZuU;H1zugdz5ER_EG_PZm)77KXGMqPruX%IUdJ^(K9!T6tSCh_z?#x9`SEt9"
    "CpqITHGf_C9y`@|6L@v-3l<;^KZ`e3L4JxsDeezs;9t~moOOvq>Iv2pm`%yQYnRUI@4A#&LX?43!j#KNywovZz(V_V5kGs5mU@WH"
    "e1|q|nOB8(*keXgw*TO~wh!}J2_H*Z7!o@zBd5w5iH}|Z;6P?AmJa*2-l}g&j9Rw`eT2y51ZO)l>$bt9=3kvD*_MAkm$GseFpuk9"
    "sz{!Eoj&IEz7W0PG_Kjfxxzk_Y$7pfG}!yj=#l*ROEf_ho6FgFpgrGNO*EUW!V-3Dq=3Yce4PZo?O?hIW4&pP)70ZCsS=^K%Y1#T"
    "YQAdXWEn21Po)y}T?S~Z*=BUiXnic5N{-E{aUK*l)$!yvf~IHO9nJc~=9m5Q{B-_$(onc~)mK-utlF!(E_K!2$7^RpAMXmFDrnnW"
    "&);~SU59D@8rh@Ga<-_S3dl)r$WYPo{)=D0Ze^ea<nkWcavj8ekR=iD1Q1@2y;^tMuhRZrWP|%-xHy+D{FMj~>N{<jbX=iojxbf="
    "u*0m!(>XA{&gt*K;^*9QruL1?r|QLA2RPm@2-3OocE+yN?^FnTK$Y|H0R3z=N#F`X9q08rS+UF)ig536M*C&vy7c(KDFv`<zcPnz"
    "dMSQ7qn3F$*_%v)U338V<+R1z?9*4yUa#A&18e@;*jE>0P%yYHe=mLNMwQwwNgI|8;984ZgjIsU$<gG%1iNJ%ZwhYD?JA#sxdwcz"
    "dG!JQU0&uF=gNt_gp?;)s~2-9!}O|VNJfJ!#@>f600ho=`z*h^j4Ee(ZGr4ImqWH7#TDpCzEpY5kyVbSI?p6*f;Qj}O2$#qf%G6;"
    "_uY6w{^MP8te$+oh!MuY=&7hD`T1%6y~)imHY@a|i1xn4p4MPDkZmo>XOJ^@%Y3p=&i*=e+v;P30Y_y5zq-vmy5$f@lh$iRK|?)C"
    "A(mcpXw*p_w|9SRH`#al`;Vv40rG2U4X3k7r%KLbf_L!ygKL45Q1Hh<;PYKCObxO?`csT@ZtrFP*oqi1;n+j}Nl14}K2WjS9xP01"
    "J%Waxg`Ae{KqiJczYK_~{39F9=FWea-}`r&QI^lErsp0TAklPq4%%)1#kD+<2`5Vl+EJ+tPw;C15CH+4Acr@0tNfBovtn*?(kPC<"
    "rM!o;{obMSBOO<F{3!8@b-mNx>cF%rlV`A??o!*m{26{YlYgkVr1zPQCA_<JqX@!(?8bse6XO&FQh!!ys7HMm+i5`7HW}@*fBWO|"
    "Y``Ru5X%WHN|R4czE-8MyxWPm*v2&h%kaN=97&5{*rol*<k_M&?{1a9!%}LbH=C9?hHR~;IHAxqE<crbA^m&hz(-P3cG###kZe-{"
    "MO~C%^1^nR{8#gHJ})2DTS1MY%>V$Gy8$~ufk4c7tM~QByqtR6=7$w}-CqN&dIy*Zgnw_$GQaQB#{tf3Y+(mWz=4(()mt9gmAv70"
    "X4$#D!Qbx=*6El-Lf>iSsNVOxl|OAUAcH(Wr|{%nfRZZG1MQQdVpA{Ibmnwwk*0*<6%>>Vm;A6mGmhOV-xc$^F!Sms^Fv<ifNas~"
    "e6+*RtE*%+=WCBJPy5YNI^AwIn<@KOw$t+E_ykOiF+gt}@Qz6!(Ok^#Y+?uznp4SHfl1g-r67FK`o&369s}G7<{3MaSEXq_jJX<F"
    "cm?o<lh0#6xvgCg8?6^PhL^1^vE~<$t3#3oY~*X`h89_zn7LafeV$TY%MdKjss+aMhJh_m1xpYPFspXxIwI%>0APPo05QB?Y1q_8"
    "KG=maR{2D%wBRcbU<?dy=h>CiOzDS>>+rG4S$|<h@aQWljtkXPLx<&Wh+!*R{nB#$+GWBe`VZZk-^NMQ6iHu6HlB_a%Ho~uHE*4u"
    "=xpP`me(71!p-^rgVYwc%ltZ7qPIV}J`jdu<eLpj8ZORcCVt{Ht<8D$;?{=0I)1{71Ls6Xb$A7{kL@n2`QJp+9g&$90rQ=d4QbP^"
    "lcGHe$%JDLwrS3VkI3QaOOe})d&q6!o95jzKfJv9Wrm{l`My=7h7CW<B~8B<FqlJF(c)}*!DS_AHDuq`wT@_olI$7`%h$xb#+c#U"
    ")q(F;tCwQEWM6r)h*b63JFA2FS7x`9E|dm-z3P0~qFH0Z-6~)CH`vOM?pVte)UrBSdcZdvg{Q6B()*Brd02_38XVzLLWAVr?h|-M"
    "2-wMS+Qj)A{8IKYsn4xNI4Ap72Ai|li^@gB5~8y+5+i0CGkXmx+NX5s+(y)P(rmX(%zyoNM*$oi@cZFR9DCQFsPc<ZZ8gW~oTsLg"
    "^KHo~K;Bq;lsvQ6v|Ht8UlUg!W4HKzfsW20MS=vuWaoJ|`*Ji*v4WZo=OwD{Ij8*&>ryj2{<&SH{Jk*&ROs`nnerqBdZYC8yQn6v"
    "RK{~^Cj{Mu>1jR)%^-N*P9HcEv=e?S;ioNGRAl1~k+xn=NLN08X{14f)tMRaLcpw~%rc+1_Zhl>T6rX2!k@wAyH!5JYUeSXvmp?^"
    "oW~1b2lE2jJ0j5^q7H}U(A^zSt1mA9#c+om<rbLgbF*3HJU&aowv2f0CqBc&ryw#2iI`kosn9%D>v_^Bfjm~Tg@<|P7tUN0C#OEU"
    "_VRN6SQ+5k5hi1-`&Z8Yf-|lB*SD%^Kf(Qg0OiaHJ5D<v(9FaVu<|v+b!=Dp>LtfAdcB?Hf>ZLN=F@DAlJ{%&HTbvINnG!|6D(|K"
    "D*&Wrw*VNxb#>=l2p2nlqY`|XgH83U+3Mo~a1q@BLLBaEYFN=u^JN46dy}ni4ck{~0TNnvhVb34^7F;kx=1~=-tEN$ibE}6lTA`}"
    "==*R_O}EBLns?}MN&;c!+Gy}Mgax}3bN)uy!cxj4=kot-9E1fb@})kqlF!7pu02h9odE+Jn}+x{kkLn<!)2!zw-0lD6B(A6y-J4C"
    "9WNLFejk!XNxTyWzae+vrd(cGr$Jvqqe~AOFekaZkL@n|tNh_M0}hdtmIeZwU>r+<u@j78xOHgC7K!Giq?fE|zl^VV%^KahPVSaT"
    "pYqdk6)L`E@G^+_cuFhEOPzf1TM2_WFYEYmRb^PBs@bM|2aq>A9JE{IS7N@L$T%a<blb<SSU4d?>*_baV2Ie-lQsUt^H|t^Ev&Mk"
    "A>&}G0JcrLa=D6Uzqt<)v*Q*qYI+FhH+pqPub<d?O;wQzCAr=uroig;dcYuT)unv-?(E)JrTvvje!zZjd)IBjlFjJ6k6>d?jw)ss"
    "i;a)N?ZUQfFQ)xpyt(d8)DHcfKbhPtEF0h=(05UD<z6BMYy@~9s5?*ncNN{_OL!}I&0fg~Bar{k!x~-OM$F&ce~wGQ5ay>#c^GTL"
    "hDYtr7bFYjd<1fUTGW@86QL4_z+%@7`kAonc72SX$KSJf_s3tdE=3OO?+bNtY)I7rQ`oCfaZy@P>D{b*l><L_3LI|oXotjaR=K{?"
    "e^6VJhrhw3wx+VbLRPsN4)w5$uc9$Fr`5*q>G+3t=N9ZpA-Y)+(|4Qf`Z^55YA~*fx@%tOAQb!hIT!fn^SA!!Ae!s_k9;z5CoS;~"
    "hX8C1IPde_F7w4Q4`v^5uiJu)QpS@LsVwF^Kt|n$$Y?8ja3Bvn1E=r$3{|^qZpE~Afa&`864O7R#W}lK?ne)&@;ZBQ+!7qZ@;qh("
    "VHoOeg+Hv8$GcmXefJ>a`jwdCGUCw^QOP&GJ#2E84?)s0L<ZvTRl!?<>JDrM-MIk>cP!pK%r`%~P<DOSCFjz7?++~Uwd1m9MS$8#"
    "z|t`o)Ct4WP=r{2*f!Al>wF-uI<kN0KJHz=wCb_HFr)An39(D?9^U<<?MfsR5S>q*vEoa3ix&Q}cN6xxlirjg;EP@Q+H6<(#L`w-"
    "&OwIz$ZG~)7=HaTE_JKCks|?1@qu3T=*Y|c`wY(J0sf`zZM)2;`{9jw)y+xkIFgFywLq^1L<5i|=jKm7&85WFqui%n&>Tp|9oaD8"
    "k+j_bKQId!*6+_wiSoP$e7ub{@(&jn|Ma(4vKmnVbY`){Vk10f==o~tuEoBknb&V3yI7X@7b3W4J`GS3kSu7KHhPx#m^YD;covs~"
    "b?9yA2sdw@jI)qJf3~aq@%&hhUmQka8DtU$#_oA5zql7k+P$x|6BI@~rrpR<mE>4ty4`lRdl#FpZ-{x#WGS@A0c^w>DCXthP!g_^"
    "L=S1fB;X9<!>LZwGayRSzNQ+lgIu=_XYJ2)x%|H_D?I3OtOjIWUrOmdw8n$59a%Z11B?isGJy&gc{HzUCh1|90^(0!)bmWHsZF7O"
    "2y3$sKFv46%H+z<QmfSJjplK&llevZrTs1V!cXbN?6X}a{w|rd6$WInxon8y*nrKRTU&Y3*3CN#2cRE3rjyJh`4iPTyKmH~U@~zu"
    "Y*+c=jMd9Xp1<p13&3;m!s5g0xJJ;kWI4bLM`fMFEN?M>>hqPYR`J%(EoP44-#wB~J-lPpZ3!lOTA_I^UDIv9O=|MnicgSapI3-f"
    "tNQi;vPOyL-13aCW1c*mmg|eExV80_y;NYe^6O2;`@H%*4`S$j^~c^%#|#y)W`py6`u=3bA2z>iWw5Zz`O}NEDR4hDb&tT*m;4-b"
    "LQ{(4vGzNZp(2#sYbMLlZH~rvl`qkl&MdbDC;G_JBAHVomS-*%A(kCg_(#I$50c8jBz(|h`S}V;GwoLS1+!+zOiGjTH|;Wpchq`F"
    "SK<CnE;@Q7U1RsL)H%bNYQJK?u4{UovrT6)f1~n7%++SQY!ASAMRCG572eIGm1T&*9w5cc<_+<%-y~{o$)>0rpB*y)W5=+fj4GxV"
    "ICz|I0m(#89Mtw4;C51F2~l+DeNczIK}n+xoAzsTt*`!H$NztS|9|uU>i@_4fB&zC1+V|*|Np=JpQ|_b&>)nj;?Mv7$p6P*IREt*"
    "I5M$F=>p$Hty^Ft8CAc$)6re8ogL`8-L#tO9RD<oS?4>n$V+Tg1q%PmdikIAAV8ewW}DJ~{tIk`^%nq^m!c%;zx@Ti>G}&$L$-KW"
    "cF{q@p>^5yIuTUtARG|0=P3u_F{t)@ajx%|zyqnY+ddt4(a`n$FHe-~KkGS6tI`mpID40WSnUVswexI1e{j`%O^(6HT6YBxT)$Oj"
    "P3mx5DwNjaditup{D<|*zrNUV3y7`<%sw6OeE1|9#EI?h@>CtN_!aC^jO%*R2|bb($d(NVZc}^dS>B%iX}y1au_mBi+J*n|&+F9t"
    "&-+k@wIr{*?FX?y)xFbDTQNUNxDzPq_*CLR0oo_@BpmOZ;vDB(?x&7EKL7cBgz&HRyt&of_rGm<SjqqV1tsbn`YdmlRz7F3yXsV~"
    "gyi-F_jz4z6?S{nTRCv^zJAC`LJ~l218@I%z4)&)nYF#G^}qgpVC;W>KlV=!OTvdNb_sboXzjVw>PgEpvpS1($SL`?qhib8$hI0h"
    "E%S*kX7B&`1Bd@PFg17|>J8<8`Sbt!{S=RgVfE+!=f6<?=P%eL0^K<N+h3^vdSfT(+g`}_-~IyZ_n#o~kICcpU*B>2*RRynd@aX+"
    "eP$rszrt2qZF~Rina|^2zfoOnd7=K-KVSZwvgE+ifBy5~U$00V#!|iT3A%iY<wG4U_0{7x!A_De=T?cy(~|b-UDk8zSZ1@_bM>00"
    "qZ|Kay??zhZ3x;D<NVKm{`{YB-h%xB-bYVWI9uXy14qf!Fm?OVmGT3zE9nmHNaYz$_`MSMu4a(;@#$HPdjDy?e?_SPH{+H3*B!v#"
    "{nr=OTVYM*Rj@K3lRO(cIJi5l#9@SRE&~>jlKlkP3T;Slh9_6>pmkg#d|17I|6#rOudq#CCNjF7zMBfJcfyYYvYoVJpH=}}hWW5|"
    "Fm%@n;RU__Ka8D8!sI%VqwkXITyP$a!}0z{D13?#JvJp~cUN`hd#<1E6ln&KW&npC*z@d-3%Gkan!m=2@1}E+7OCqS3x8LjNTbir"
    "mD{G{So4Jr%9!`eEU5E$jia3Rbaj1yKK)(e8Aaa`udg({nIMb(3(;SH!JFEwR;l8@Hj(hD11}e4ZtDgvLVj}NGnAE@(1;k0)2h^)"
    "5$q9-5y_37vNjj~%2<Al_ZKP%b922Hue-eboiXeabD>0~V|-cgQ>%}y!NwJ?d9ges;i^fGxLVLZGfA)qKXU7#52!rE<S*m>oxwuo"
    "#^bho`TU7mT!Q#(RIG36jWoC@4>)vNk!app&{_Md+wIU^;O^RN<af(+tg|Xof_+zuAI=uEr{dx}XemAYoxY6c2--df!iZKyWTLo}"
    "NNA$d0Gs_3#hlfmnIWL)*=9i~5VvI)GsQaf@k09;?}Z|WNTlTJZO<w8ybqX1F5$K%&=L5HT@qFw$y^^eOHNttk0TVKd%-}9HsybU"
    "qGJmPnf1u2jotYdTaex;Oy}q}0!c%7BPovTZ#3*f`28aB-h9ifKK#wBFbaCcZ=a99M3gCyx_<r*YX!SnQ?4`iMxBDwilh7N^Yfa&"
    "zcWl7O?j-tFAuz6CE~GZERypDli|S?k8-3|cvb$J6Fl#Qh*MB<w@!HwRk+qFiXo2;fd1)ts@p!sd!d3w5i4`iWg|Tif$r1z^klA5"
    "TFA9Giysh3-P5@55QNxyqe*TM_NkVg@tt|XoBExP4l6@s`w9XFoYp)yoeHZ}8@TJwm8zZQiyxTlieh^4tx^?M7oEl;zHP>O8SiYO"
    "MBlr+zA;~a1VCTv3NOQ(8bM9sOqICx^ocmVI6pL9&a65EPLQi0gx%7k<kY)agUb&zy@F!5{tf(kUB-LAV4w}%UEyhXHwB8%7S%uP"
    "bS-v-i((s1s772?5X)DJzGes=N-xGKK0|2{3T@({Z9Te2#a>oa`kVRn=Y(Ha!PsK1iHNb*@GzAAXt^nCRjr?#DF-nlJc85MgVvC7"
    "YRp&#ueH6qjQ1WI1dKqcxo)vkU+|Og%|<#7+2&$~jRk?d)Yc#q=r<3s4D|3bmg^A*r`GlA^ev|{H0Oxrk8=3Oc)1?*nICI~+ZQbU"
    "Jv7h~M5_UnrjbM2e-4~HHh^Aa-tit6&4y-iVwAY}7UJ|+^WuJxHtwnIGTa*k6>x~=Hz*bA-#sO61VOsxrD$I)h;3@(W{6@B=No2l"
    "RQfBwdXGe|5xOoI_ztC8@HGZ8TJe5nWnUv6i~7Z7MZ`A`T8eLMxbiVAwlf1-zS-PnAif7<`ern`D~eGImKwFQT>BgITz5~wcP`a%"
    "GsC7yuMo4XsQ-=TrGBJuIJUY+PU&kr|AJSgk;l7ZM@#uv6kXawf4xRzmJkH=n$wvBRd4zx-Sskp&<rK2PKjhCE<eAQYz%+LqbNkc"
    ";}&9KI9ORTn8P*AiRHYTHq$c59u$P0+d986ZsCS5Ja}5mC(S4XIm&npX|O>F=*IV9pH@pMlP*CCU4lQrKtQLUKXUtm=b$iGTOO~("
    "%je%cf$`ft2*neG#7DFMD3=HZ(Uak}c7?d)nnLdBQ2|D%SP^s%>XkzFJ(O6Lls!|8wp|i}`yQ|UeM1Qsaz`^0@*xJ#&5A!M;_C{{"
    "s7&NalUwM)aryW_=|Cq6XM>G)Nn<&i{Twe@{|3E`kPM6#brfN3goQjpoNJ;a-@*7Mb|4$2XKf*^xHm$iS{-7;%8gvwU&ed0u%Jt#"
    "fA#VL(i<eifW?-$;87-i_HNxJQw3e^+L!~ku5aL|w4$Yt9uI{mE=xzDgk3=fYrzY>OE2)j#W#RPZeQ>{47aWprNVTJV(Hye34`P0"
    "Z~QU6M|H>uID0**ODUhQnv^cD3_86>dq`tEH{G`fUWVy00x?u7A#&Vy8D<aDMDY*`aAG1O4vz!O!aG2}HvbWjo{<wzJKWMgP3;jw"
    "`v1qHzx(f^p~lOa8zrN;`7cGqIP{V03I-SZw1mNh6c*O!)=*bA1ebadI#-_4hDipuMLfD_9iu!kB2oN3-g{2Ne4E0A;1s;MSn|SW"
    "E(__?U7_tJQbwU3or#aGQuZ6eJ+IeHCo8H_cobj8`x})ab^4FmxnFtj7J5ru+FN0@8TJGtJw<R)@k*db9I;a!?rGqe!^EctQqbu;"
    "Q|EI^gd?;!=C)?@8^<xe;z+|?AE+-@shq4`KcC*X6>WUlnQjl9<7jMno)(VPeOe1Y8)~POl!ebC4sd<$wJQ=~h{kBFgpwT%q=Nm<"
    "CcO8`Dwpy8u30TJv6=c*CJ1-zp71nNbkg*fY0y=R#&m2c6~VTr+N07sCz7Z|xVxr1qLy=6g5~dwBi>K30BG4G&m}-+u{3Jt@m!!d"
    "B3CYJ&^x5SJEqK0Z?{Svaf<CU8ZY<v5-bNS3b_U4pIgu<zsS#x$ODcp54;2l-5Jr0Tdpn#PYNHWKkmGA1DQhKnG+bFPHZ7G8k4Bd"
    "+a+)wT&Y9xl)obbvL;%HsZmW6FXO$Z9_D$+`L-*{dt9A36bzh&q8jYNstc0@M9&g}J{XlZjC<)Ng?<Z~CjApZ@nF#!4kGcUdEYPN"
    "z3&Kz&yfFzFPP7+64*1>Oi*4DsR*bwcLkv3O*KKu70uo2BFKNgQF=gBeee(!Po;y1gw1OZNm=<35z(w#L-)usj&jaKtC?o3qHp8^"
    "l0)MgfR^&!qxnMV$#uqFyk=o1_Ks)jInLz9&pyor-w&Ks4ZpDWTbt_Mm#ELy;I41Io@_}49WBPU2VUA?A9sdwz38gvKvB?HY}en`"
    "NsZ}X7>q0_A2f9EXa=+}xfw+Y%TTeBa5|rh`KJt5v7!m-IiEa2dkXe(Ad3Ax-n)*gVtssC?=GKzm!;@p3S95fUb?**xvWiVm|)dT"
    "V<P$3gjnN306dz<@~BjLtOP`a)ld%xN`H@6-@_U!WbPkVz^?}&wtpYMfX{I0P&updXp=5v_OzGqqN&*4B3&#z;uRUUr=y4lfH`B_"
    "+R`)7AvM-zyq6~lNYQ^0Z(r~|W`C;HAIG+)G#vfho08zsXfL5LyhkoWyuMnflO8Q+PzcjBc#rl`#K=!+vR}q~Z#mdeG3ML%dt<fp"
    "3e|?wr{dvtU67&D+M^eY)JwrCnc8SS+@p7a9WcVYWzc`}D9{a#YuaVJbN5Nvudmk$e3`E}N;KTv+#bj;0<vVCbi-r>v-iYQsMvDV"
    "r){=07)?{kL<Tt^yJMbXqaHDO>+H`5Y`U^NN^c?{TZ_%2t-8O*dt+}7TwkKFJuvQ?17niB61J40bV!eOw8=EE{x-8r;nmR2$I&J)"
    "bhcs|)_f0F4<e*7Bi}B1^uP;hBPIjJUP8CU#N{t{)$%Pq<m&_H`nEz{^h&i4a-e3k6hS;CL6dgx7V0e+KEpy%#w_BE7U2r`Wntos"
    "|AqeC_Y`QrzccQYk2RXd)=is={&+!>u+SXVk6jcoKW*b8csTyhhcxj!-iCrhjtQ~Vyo~lYh()dO_%zYij8N5vJ8DVN8KPj0-rP20"
    "<(d(&O@1n-iXmWa+}F0Fnj$RcqyHYxy){;x^3`xV&lN~|%RLgVefj!&-!$ly1dWR~^rHvf7$g#f!shMs@y)ZCuLf(czu>(m69KLS"
    "_}k~-%Xa&eH33NdBVQ^Q>J0+sn-kuoKa32p#o};frAOt+>5QYO65aS2CTFQs^?r|+UjXcI-g?{(vOz6DNPKh<t)XAiiz92DiLLA3"
    "aD(4z18P}#xY&luc??6m&6|B0@0@PIuG6h-%i5PkS=?O2;r#7^b6$t&V7lExp+5Q!fd6ui*9YEBqfdLW?|A+E3$^spw4}@J7r3{w"
    "&3k3v^4P#mx94M4B~oKX^!e##4_jd%WToD@v=@cJm8NOo@_BMx*agbEzOTEC_fi01O_b`=^$VWE4vMhxDha~(ts&u>sW}`%DUZhE"
    "h$s%RlJ!02)VGSrCN+f6hklA;c6T+D@A2ZfmJs`@U_0~)#R&VH^|`4LD-lW>L;u#}??LOlqDK&;r@h7O?8f5+(9KTvvPe$}4lYZ)"
    "<^TStkCKqaL0wDaFvLx*v{i(+VyQcGue_dH#f5M}rS_`R_A}QEf1D*g#(R&ni=vk1>N@}Ly_leG6(d4orMKZQ>B}QipMrNIy^nO9"
    "<8L=RRybNXC|zazg0#7njv~$QsfF+H&K~4u(qXxL`G%)Nq$Xshu1Ltg)vbYoA<@=wec-S>enh<0Vd;SvD&TdB`q1qG`yO#>@fyUh"
    "uMeEtKu0B(y*}`sRya<F3}=Qy5mpGoWyNm=Uy&}#(`ONf%6KtRid+=bSz$;Fa2BQnfUYWfaQS<@7eEJ;>7Mf_TNkoula9<e7DYj5"
    "`wLQv1qF01B4$>uMda8G3cl1ohu#z?N4N}kY89!}j8|~x!wb$(J}bdp-{~pob)s{@<m)nfZ`-oyWhz%hf2v$O2P+}Q>i{RcSq@*U"
    "y*^M5XK)miyX&Gkch0D+3k2lr18*TcN2Zlvp=^N&TAGiUOpID#E;p-pziEuwwE^q3pf^2BsAMDFJ?+zT^2d(jW4xCb3oaY~$iFKJ"
    "D=G>qxTREp9nW41IqroaQ_|m^A!9kWLlksX0MPap8!weJqdqU=y^BDb+F5&DX5r-k=yE8EQ@-XXJ+~a_HKD17rL!reERpXb(C-oM"
    "YP1ZCL)j+2TFQLFd=YHw_A=f}{FVJ{rBzK!uTQ}(+6QrkRQh0BiNW}aFl9l^WhNZIv@1tS*^31z(`)2u$9Fmp?`e|~mliD)r_8=n"
    "pwSMC7a=%LmwnH5d7ruqRGj0NIcdWw3n9Q&8u#;pc<uWFQJ6cNLta<1y9Ube?(>qx8%;>p&%bym2cH7FT%<Djp&XBoQ&Bi?r6ZRl"
    "R+>;nKtMbT6H#c|Mb#K4<UAY7P~-c`TjRYe*gc&VPmMzLxKvaxup3s#P?Xscp|)<eK2WXCYwjrkzZ%WEm?RAW#Gu%88Sf1mFob-q"
    "3gIgFy_tDV8mMw{T1MiSwXMQ2bQ#0qIp0zl*-YP2KdEjDpoliMj5tQ=!(vFHiTfVz4N)lCv%^3es}GOoh_))$L)gUN-#1X$Zm`3&"
    "g@Kx&0`>^PhN6t7L7+up*JZr-6hQRcJ~5X)pq)ugoAsjKv}`X0hk#|d;uWGK!3Yd1boFh~Wk=@8<NoIV2$g(|_rl-G#VejDE!CCX"
    "q^~5P?)w}hhf*kBrI1~*vT*a85$2w_C8Q*_UG?H|fV>R%hJ%~}P!qKej3Si&6dW=)Oo^sA1p>QJ`oMEbXh6snLxU1HEL5sqk0B?@"
    "d6)6tb3n9q^YwUJ#PYCo(l7-NT4j|J8AZeysF*HxEqyyB4m6K6(F*Sozpebo$*CLRQsHg|n4#@)8Slk@{AG@NuV1jdce~uRa`zl}"
    "U$c@<1YMA)*=SmpDWn=W`9pLZvsx^shM;n8Wx1WTs`@s=ea~RD=U6J07zwEK32FN;+aWR?yPLe-LWTjgXRVcJJ4H9eXogHcUQHxL"
    "|89ihvcHV?o&m&6VIfT@ZKiDg7-Vk&@+!C&DGxJ5^xXsnk_&WJ(R4Rk(0A)7?Ko|Z-0Wq%7l*PqsRzHkfH%XG^hj*p0#@{=TEK{u"
    "gVn{|6j*yw`lanr`E=9w+bOKC+Fc5y7A@-(w5Jr+7g{>m*tvSDX?;hm7!Ihi*jt(F4|>my3PK0h>lS_ffUv;B`n>i)!>ef3GT?o&"
    "ZZuVqV>E(QR=cA%ih_K{0+qU(ZqY)_iZ6oN-{ZwogJh|ie|(!#r9a!wZQ4i(J`W^1l)N$qh^<e}R?&XYY;pu(vla98%7HVQ`pp#L"
    "Ifc@aAa&l((c)W5s?jr=X{f%%jjUVmJ|g^G>p~2PEn4y(bJK=HpH6?e^J#${e$vL{1HDt1@!s1W@fhJ3l({t9Xi{d!)<eHQabP@-"
    "cA`_;fiwkWxSC8&GqIKZ%2a+9tw*KpGT!M%t-e0ZhG@)cVr!%bP3r(HS#370WQAre2~I^R{KvLi`Enq3GxEc@pQ6q0@$zXnRuaP3"
    "_4WeFdt~9b?cs%Oh2f~+D?oyjJHlZOFELI=s~IbX9>pbV!K`FZMXl+#e!DSJp7Zho4$7wsx86_9;@eB;Bi`7MUBOeDf~qlp4zGkJ"
    "_KMpeHzkCnbV|LVa(|@SKpCivw(PWzvQ}xI<?Bksr&CZH)VW)XO6FXM`f54bxjk^Av_WwpUN3LbDX7;A@`7pCujEhrrTKHhfe#n*"
    "gDooKMygVFwkwPl(51m(&t~ES<^Qe<vzFb39kO6r?k!$s?7(tSzdw8U^7(X%lPNmAvhVa>=~{B6V-xvpvl9SyxJ->IG9u_;N2tY%"
    "dK8Zxc%)E!O%bM`uchFx9Eud(u%leYOQ&#9dzL1Adbb7MCUIJDfZ8+ajF7n$_j?-svlUY1=tF?nJmU1ChQ;CIllNu3Qy$|eB~RBE"
    "{9fl#{Hxiy=AHCHRga`H=Oc)1qI6-9q|I<PK`&MgH}TF45J`0j!L~lT!s8Sx)V@dZC@VHxhWlMN;tQT0A@|f5j>={jo5$FK<2ftN"
    "2q|u{08xCs&XH5!j|S3g4l`jfGgtS8c%}L>bBv54dW(_jI6-Rth!7NMXV%7^nND(n+1(T<1)ZNGEBrn-67$acDmD^E5B4(NTO=qf"
    "?jL=(hMlISD5T*Vm9~vSmWVA0Www1z0{RMlcdd=K1#R$vp~izc#qX$|Uu3PA%eDV)hvjpH&&y2pR-~b|`=-VG^cE||7ALNtXs!?)"
    "-B`Ila@&LBYIL4>WmyqFS_FiI!HRgH&_gJcs*UgY%g!FA@F->8a8`aV00X*uj|g2&TZk4N@WzsfO<8*j6gG;8`b0Rmz{hCiGTVz}"
    "3cjPhtUZYV>}9<89z!bw<~tC~N9ZG2gI{~TK5(imlJW*RAL8ECIy8@>_Fhgq$ELN!k|b#)iYxSM+a9ao7tgIp<pd!kT>ia7@;M3-"
    "3`zbGq}IOVa>d)OT%46Rix9PM>{I<k{f?Gk9zp+Eq`VdUFhdnt(B$R|^~tA9NT5X{-fFMyDKn+?Olb}^J0yKKF2a}qVm2DH!P5r&"
    "YC}N+8rxlyH-_y<H`-h^Ic_PCq<EQ-a}|OEnqvza5x>-)qaQ6I!>zpF;7*STrGEG<yH#|QcG{i#@Ff5p(C1{&yUU6?6%mH(Vq2~&"
    ">IG(w@)Y}6$bCLg-XOtPS}&B>=lwX!a?O;5aCa9N3Z{jjbk#6V9NS_&q{YTFJK$}pY%mt7D_zDrg;Uf|QvauzU@5v%yhA6oW2;pM"
    "c5FtAX{{#Qp_f(3)5-yzD!YIg>c*x%e+P~7!k}7lc|std&C<A4LddcSIBU4@G|?t4Ak-$sOJ^AxrPBB`L<|-C*9g=*d{_PIPudq4"
    "3qn4!x7>M%5;%kS4)=lj`>{~-i*>9<#C>kfgnNj_(>usB37tFz>OS1YOTTXX=d@jzzu*@!6cU!!wb;}YU*lRGuPO~6ddXsmDEV^B"
    "(*tH9k9EKhJGu;a+H({qqr0xIx22IlRD+M@`oMcS!+pAYwCRPL($s1wN&ll4j=4enWABnT-I*PWy)+enc3OCP6rZ(LxX)VYvK-$s"
    "@=S?{^|t_A4pj{(F+z>|AHnRsW1w(`Qf<9YKOHUT;sQO>E$7-Qp`$x4b~U!BM)9%bez`0!`P@|E#luagpnnv)-U0xTn-bR}uF8T4"
    "ek42yH@)za@{AyEB~)YuC7qpgLza4(kMe7nLvj!U5?p55^lcOrC1OKflydCVDCx;c-VM36%3)9uBwE^2ol6T5!*)5ItZ&NX^i?_C"
    "-H2Uv2c$eFnzu#)ZHhyj@Ju3t)+HX(;r)WE`uh4=0(x!~1@WyVUJy^^<ZEOk*Al`eA_U^H!Ybi%etn8*qetp0J|3>Z!v=Fb6Y!?5"
    "k!uXra-^h{CP1XbgsoS~Pnldwt0@R8#o#4#8Lu9B1YwTH0fW+r)N#16vnQ1O8!BCGP8^QYME24fqa~_n9;l=itk-4|v(_5=jyUy5"
    "l339Widb$h;It!&Dn3PouJ*P}uR{oAE`@eYcG<#?5#iRVjOiBoG1xW5Ha5k6bHQJx+0}jr{Q7HV;+V2tD|l)-sz9840dj;@GF@=U"
    "ov$h|RSkO}Z?C=EajKbXJ@%6Gk(de?<`fY)%(LA0y}0tgTUeU(T|>E*2Izqo_i8nwja_aJyj6YCLB&}TF(_P%8!3}7Ee}7rY=lK;"
    "Ou|r-_9$0ZyC)tj7_*9duxJ9}YVLcy7noypLyE_>T!26nRurmG5u#ACtx}pbGeo^h41GBkYMWw%K*NRNwc*Nk443BjcxTBYLlwev"
    "Ll>3DX{c__*&c@$IHw-k)=#Ud){^oFN0$_|89ZelKOjvGi7|bT_bYeLTor3Rm$>{|$cJj5amd^?ReC+UC-ln{;1z5-9;$yD*7chz"
    "hs-nL|Msmw!g>B1vLIHEtHJjJ@l+<zY?o-q8<fZgzzfEXr9EeY;&_VE&X$D;zu#yb7_!$W!h5??@0~dZ{~qqG;;Zzw^SbrBL`R&+"
    "YN^Weh$<_#_7!r+Q8<jLuQI6Hr`Cja{nfhO40Z9{en(Vy>XteB5?Q7PIXzNSdIOx^VYVQ+Hnq@DLRb7oCq^hCrAHANk@uoFsnr(C"
    "V6|PwdyhSk{WjOS4R`9K=`8b!gA0MyR)iS25)ju~%J*9nBGBxGaMRdcY(yL`*AA4<LY*aUE_}NFeY?&7Ss#H0<9H1SzvEhkIFirh"
    "fm67Q3p`5Z@%q3kkB7CVeGHNYj&0ng2>z~VG4~M!3U$t6{X$*B7h(7wm3hrB)%5KX=LpQiw*sZJKP7;a5%joikyG329gW%Me|X?M"
    "Z&Y5un892hI0vpzp^d-w-RhT`-yv{B?e_Y>DQ6AQTX!pGb-w~)WM@33llX|v(SjHa7%!t^ow;6|6V(-^Jv?kTh>`8(0|k%%IcCU0"
    ";Ol$5(~Yl8DmwKvYZt6yNAKuBX=%IYG%kjZh&~IihuNFKC6_{qmbICePtoF)*-N{O_b%*6Wk$WP(-*%hJH+~w{Np~#<?`c~)(F;n"
    "82|JKlRE0wn;a=@p_e)-%h0Elw?#_32uQh1$cbH+Z2{fYp2fNcJ>`vP%jgi?BA_OwX8JTiL0D*g_Sh6H9#a;h38Zw`w(r_bjd$q0"
    "&D3e++l0IqB@rcOrR?i6f14nJqZK&aw4Stae*J5xv|i!MQBf?p(uQ+e<nEuQMY_24_whoh083gzl<tB4Q;<`9lt#tEG~1qXr{Dn*"
    "buBi!CNjw=e|Hi&mrf{oj@UR>M6tCX@G=y>z=p{p&x$@zc{K0coLlhAB?<?;P_KAJ6m_c$kDbeSFXgEmtZ|Mj<j(m_6fLHNdUXI?"
    "n`1fdwejr}ASrtPT*QomP=bQ+7?`}&c*(a%;Eoc<M0~xyfL8~HqJi|7@L)iT&%*Kuc|Wq@bqn{1TwybWu_X5_dSTYaTAg?^fJJlI"
    "#vFZI#;ZrH3t607oh1FkZ(|XiI2MY;0i1_Mua-iFM!Q<GGswhivqiD$F@`prvJ*2Wk|Mk(&Z2kreSOOv<$IAqGrqsw24b*xBn5MM"
    "3|VVk$bxZ>*u@lszC#m4XBpu>%C*#PYY4x0)AawiE5It77GW3W_lokryFmj-y4A!v|B~ovF(2~R?CF2mQvp4ddJ8K)oTG5GR+ZfR"
    "^@;cB0G+DG_Bq>?ZCKg~wxQAN>g?iXR%kdm{uj!2zE#{|S{1lMs?hDAMH!9e^fKOC(w&^9fV#AW-^b84P8K(zRj@@VW}2CoQjJnr"
    "PGxvPlyb_Te=&&|HG?mA4wvET=@Sm_p|SdlIS4uzAto(c>)mYa$qvcVvN5#hgmj@?mLhInx)yFy1l+*P{XO2>_2Be*Zz!bQVk~0m"
    "kHKBey)OL*iX6PDVl!)OK?#92Yb)7Yi#1@PG@e7$v@ayi$A8Xcs-nSllbo<yQS++#x@b?ck*BWVsl3erm2G^vgYx&@4t0(yUWY!U"
    "5y4Mdc++gf2_$zD4t_})+95kqzKOkmuS>a3);_-Nk?@xMN7_h?x#sF$=__99#osjY5L!5$;Gqf~mFolLbn%949+iym2jaVJQ75NX"
    "^!7kHx`}@pw%JAl>(b%#%!u7jFi&C67=>F;UBTmiiD#TL3MP(Q#2bS$G3^pB<Hdh8&0?|FU-TYV(d?4aO(2RQy*K%3iR5^FCqG0O"
    "Q$|zj`oM3a=#}JK!J-TG;Ds85pUjPi#M6_8Y;{eyl4?D0qC2%f+(UAps+{h2Qx2<6L0B+L<D+z<&*$aq#I(q93Zd;~>`=TE;!;r4"
    "Zz`AZeu}eRD<g2AmDh8nW3a9$t&$Xdj`U>ruzMHBY2C4l-Q%EykFHoa)#i$7-vm3_AEKo82!~`X(aA}J*ioN~k)iZGEAsbi2nPDu"
    "6eEl0E-?v02-k}`Y#b<1h^pV?9mNI#hCi2LtGGmC8EP{^h<30U7l3D4(GFu!)J$09DC?)C&^xf^<LiZ9zdK`Hd~d11cO2N<-uKDo"
    "6grh~D<yFMltd!wR`dOV7hquMRk-I!TI(xy&ET}kp_&D~K4%5Nra00ZrgEXZVwfxAvWnEjax^WM-4r^$$2%8O6w)Yi-Qre!C9k-Y"
    "N{{5#<@0YCiFhg9_h3DS3!IVxu*58A0(Wtu9R9S`=}ll?qjeGX;|L|hAG8+4U2F~5suquvNowLepW~%}s*HX3Q|_h2Pq_}F*otKf"
    "AWC(V*t^h7oq?{M+4Yrr4KzD!qqUdY#ACf*COo~;?%t*73rjAqlCY6-LrdO{m_%dnNNGyH+jF}}cX918Yk5<nj#U_?G6K58Ux;uz"
    "-EQSEBD0){YCtMMnn-I(&F`9f8O>of_Bc&Nar9m?wQdeqLWQWNSW?BpwY|Q_d$&kb%u9wXTpq!SL1#kMfLV~pH0PSpL<(lOr*?PL"
    ";zlzG2S2mo!m!1<-p9U-cbZfSHyHL-K&S_PH`7qV>xxW2%`dcxo^#jB1MguIlFor8tVt9|?m_fKuMUT|+LZIEz^L!;>C^Rn39<_^"
    "azno9N9;kdQy5@<_szPLe#d8|#+xX#9(ZXM!u9U)iZX)j4e4a3oDn8kh7R$%)Qaf!TyvthJ7?Wd3BYF!(^C&dgcU!U%XqKKwYImo"
    "<t@{n0zFR)2u+%|2g*6#Q!)rwK>@DAUWl3z$-AwA{Fb#w>_qtiR#ZL?JPruLlsP;6rfAm02;K@$uic`XqA3XPRs3ixCjcDueY?Q8"
    "e{}w+E~!@zm|tL!6&Bq+&72Z4O!*K;yIuIZ{Oe-PB3vx$**Fu7_&wxeJz6>FVg!K=ABIps<@E{dh=gf<=Q#!%B@~y|JZiX%_r|S6"
    "0X1hStB*B%$^}{4=(hqhHqF{9&NcxkbU``1m|H<)My$N`AD5C^N67H|tl}sM>c=5*t?|%pZ9&n}CA39Cfbr?K;Vi#UwH4p9q99vD"
    "NkPlQt+w}c3U;Gx@jc#~czwb7^HHg@Efzdp=hV*;s)~zq41;C*>MmoI&~WUfP=9e`ZM8qq=)idGo3tO9KztQ&s=tBfe5!cyPj*-3"
    "c$Z&bkic=4Gkh_YrG&L<L!g0=(mAxU^VYtxp>L&cVK`4O1rs-_!jKrS){3R@_cGp3S5VoW>+0?ms6<s!=1EgKQ>-0bNL-|pKZkLZ"
    "4vk2&5Q?A*oCUREy;d!e%V}UPr_5N7W$&+h?3LBAkA&L&Og_KG{Z_BXQ7CkM;7D9(gJgT<T;&a8R-h?)YCw1|U7#u98HK=!V!EBl"
    "p&zg*Mvaw*%AEDGw5ah$L1W!HB_>qIFXJ7);_7o>cl(0J{>pJ@pRR%IXd_Z$65Lg?)fIOdB4V(7e2aDYu^ua+kw!H^ux!@e@PH$9"
    "2!7AC5Wbw^k^4~>t;&&hSuE`@!@XztC>c>&?M3rltIcS4Q!5V>v~JN<8M1F?#?eX}_?If6u0=&|3o!~4Dja+_AybX{mMtZRS{3~$"
    "!PRal%%TY?3&Ch<4QhYI$rj4`wDeAwCKdRY8t3hbet+|5wtbIxqGo=)<;?Yc9Ubj#Pzk%jwesqrCY-a}^@#fvvN@n{<PwDX%x<#w"
    "J-SeZ;G=ij{U{r65ZXHQ9)&-#trNK3zGTc$pZ0_Ots4yiK4(#AS)xE&Q&*}NMCxA1b}z@hH#LTxW=RV8(=r_q&WKtGdQepaQCz02"
    "c>#N+96?!x7<{~8qjJ=}D58wl1`};md-|g3L$lY<|2#=*pqc*mz^mlLa&^zGy;pc_;{0Z#M!*48Q-U->7>Y6$#U|g1-q7rtyDrd~"
    "Z5Hd=5F@3HZ^~8QEiZ~FQcdd1acdEzUbBc54V<N8&rzq|`I@l=58K<ADlW?5B0cXYGewchQRij6BVkg#sN{V@-s4HcgR;Mq>iRZ9"
    "`iK<})!sEVEnMuQgP`f$tr&!RMWI)tB{prGH^rb!#?`*k&EtJ_;+bgpd=GbAXGBzN;eP1$1<!r7L9rGV7#<7iESNBy$V4h)oC-%)"
    "uf99G(%x7&?>O-DMo+#3Sn$*;h}5KikM|yNquumJ@iriu=%K7(`9&qn%&T@vw@25$#>G;DB@Pc1|EW@<09*gKc{{&dVO;$+%V)Zw"
    "IT$1U)t-a?kWyoIAx*s+MR0tHE7*-U6atEUb}foPQXcvrZ_OW*`mgccC{s*yk6a0I6*R>s<%QjamL^hk6+7BUiDkhNPY}FN*cSB&"
    "?(v3>ApYk97wZe=Y}PDga}CqA9LW|Ms_OlLqaUKJub+2c^Yx5Cr}6c^-Jf#75*#X&_s<_a2nf>c@Yf$@QCe0)cJBT_{kJgYq1}6S"
    "O7FHd<Z?Sq<WMGNDf-Gyghn7z8+SS<VlcU-w0Xq1qyEn}c7Km|;w;CN+1=iW$juvvN(lli`S$r!QmhRKATPHEP6Ib5KX-WNP5eo8"
    "#YifG;04@%@H<eRZJTCGq>eZ#8Ok0Q9=Avzu(;wL0z1O4Zsoa#P2qdE_ar4C0bQwC&VDOqS=aPFga(^7mS{@S-xK9(2s3V@9KOA<"
    "gr>|YzpNcv4~WVLl3&Jqm#7k_;HbGx#{oA+8L$9u#Hs7kpQYuMt!W1LU!)4DVN(3xttWIPon~rcVv%xpc>8IwDO&XJ@lF*@L}v9`"
    "n575&?`F>4%h>&|gzqt@Oy;`g%I`Gp(bT`LxpEwJA9b<Yb)@pqX~+$c7=8J?{RwwiY^rbfvHmA+Djurne6xbo14npHlS(7o?SXS|"
    "Lj$8G_Vya=tF4V6EO)%u&)Xvt9qs=xN9Xh?S~|)zEv<Qb@mA<QHuObdFSQt`dR918!i<9E$XslTaIUXpZRMk4Twk+a#yf_x)UE@P"
    "XHX1XDOT)zT)>(<_JxjHr=epj*v{4~r+P^M!Hp^SgyxF=V_V-Roc>oNDdHMzvA2zKDj1`1{2zdnSKlKn(GmUn8D_}+F>!>aQoOa5"
    "hn*P3Qv;Kbgt3~S1%VOp?(KWLc!;&;^?{^IK+ATKBy^<BN_Nue+e7bNLs@j_6jCT!4SwRcizPryD}=aR`#!<MIsEK8g{^#J{{B}W"
    "h>yinp*aHGTV8L%iZmq;PTPX$*{<%EDNUO{rz&1cv9aK}7iRAXwHK;gE?@9HV@B&m(CE5p%c<=b27Wx2lNGGg2FB26BD7EQ27Ql$"
    "Oodi0TRO@UOK6HC1Iy1D`ik3go-i)sy|vT{Pa%K1b?~RWup+Kn%Kh`NV*%Ii96&}c(pqmmpwJ@@q>X3{*D#~E*xjaxIn(vdQSKvj"
    "qOY~WD30@9dl@dB(lY@KXfn55Nm;y$k*uDO#9CU&Rclo{HmqWIjUBt>U3c@8k1O}is4yT>w!VjZuk|uc2I=PJ>wjvo9_vOpsRQLJ"
    "tE0WXvJa^(#7sk7%PNCrg%B|Xx*1)lO1y6Q202_@Plj*xCCoMsbfRG9Ru?8>=>O^WdZay{f`5ey(TXL;=P9c65wE9^mJtK=6v}CS"
    "V;7edFEqkQbLP5o(yXS-c<Ee?YwO*v#=Sco+1U7L_g&ZD&k8{S?cU?82g+&5^9i(FM;|_SBYGPZat%gq3g<#RnJD31j}xSgrA?r("
    "W+|K}SKQds^lyDK|1*S6&q>vz+ez&q29%qwi!m(S<8#`s91|$-#>%N~{hNhtiE^3#&Qge7ZM%FK?>%tGY&U8*U+3eE5K8f}l$(4l"
    "+wHZm<7AMk{DN-toJQ0U_jK(?DDuW<k`obej3z3d2$m(sMZu(RA&O`!4EPI}cJ*9?ricLD{R`$(d500jO?N?Y#Qf9r8&kY~{s=)h"
    "(Y0{@d^$bT2(dkW)ov)<cPi;(7jJ86I4y~(3G^6%aHc8xO+!{45E3qB8xVZ;%L?!Z<|)O~@_LI2r(9M)Mz5^gM7RCV(wPFVhkn_r"
    "&^+|0aweLE!z@&bn?;;Sf3+!fm8HM04>mDa?W!;pPlY~A6)C!m_a<2pIi-#3_d6CZDd6&c|3NPUML8!w1zXAy<7w|(I3%~yG4t`m"
    "?68#BR-u^{?G)64wc14?7U{b0(i@!xm+?;Dx942swnvXtuE)K|ge#>!)vke#H%B>&a7A!7J+%ZwD^b5iywjm-h00XM1#pnETqoS@"
    "7MId>LMT|_Z(Y{h8|A1X(6l@k6x6b0p~}3tn!`O^^l`QlgjzcAqQ@2IKm{Go2{RnGtu+m?@NZc}jd%DS2SxREHtKyorhiHR_2x~X"
    "&%aB&P|cI={eic>So^O&74h8!PGyuUJO7O%`w;pnGbttxt$}8QVyNCIdM3A4H0uJU0_Xi3b^3A!E6S1({+`FAKNIZ-`ugK^q8xE("
    "zQubcMk8pSiM%AbM@Q0s-=#bnh)OG$4fw|3jIXS@^0cGs`*e^zIie~J7-fSX^e@<NCg(~>scD00#m#Qb(A$r+(IT}Xyhs~bEy$7z"
    "GJTF0%ul2r-_)B*rc&4GlgI9kh>~2Y(--_t#8>pTN}tJ08Wu4;@0oZemJpDI&m5Q3KwXj6_t=*kOn=O>Eh5vd#OoJK$JYQ63_1*p"
    "h+1fyqI5fO_%RWS-IS22+`^K}<EcIMej~~kL*T9GOYUeYoBA@|Yl}dLxb{T*Q+JH53e`3C`oOvOMr-}5e61jRDzXw<eK*y5f0Snh"
    "N<8`H`uX>gGjOcSd5a=`Teb<RL<L4gDj!3gHfxJa<UE9^I-MhddNL)qEDW<#SUb~=5b9;TSI!9W$EzHaK7T~=5pNB*?nM8Seyvq("
    "pxz3&K>BY*Kd+qwq-hnCyBz}iu?(ehzkQ!Yab(xtMf3&bQX1lS;?P2?dDk{YdliHdr=^$ij=^7~74R0cPhD*c1%DvDAtflbV4giG"
    "M^!gm@`|o=H0p=+a;ljaS+^mKHA!(6C4r>mB0Tl2{h;xFm$0Zw+{7?^{CP$x2WgjHXZ)>h$E>edTHl?58(nCf+nHQuuL8+gUm4%Z"
    "P<GyZNAX03rePk3-@Uu-$SIE%{5N=xCyjwr(HFdZ!Jn-s6+Xtq>jUZ6Jf>wGX;y30Of4;=Ef&(V?Uw4PrKn$>D>Dvf=R%jwqi#8N"
    "w#rr>Z)X96@6;FTsnigEyXzK;G5oGkGpDlo`au0BH$*x(s~50?za|{ZxW2WD{b*yPjdl~MpscobGh&3xKb5~hEJzzh%Q1hC_o8XN"
    "c8Bf2?fV@Q_JYyY{fH)obBl^RL%mYGbj)B&45rYtv9To|Blo=@0X@;YHD)xA6Rxdw2uf_&zVX_62I&>+@ii;jl?o8w<DF~QK+QzC"
    "ncewRPIgSlCtUM=dXM`vj?$0wnzH!FE9MqQ@?-fe6f{#+^e^<84akY87;s=%aWjR3)r}ZhMCR69hKs-IY&5-YkuLvayp&8`xH6U_"
    "E*JzY1R?9~o#wYYN??|Q+d`#a5J^)e+liadM4zqQfi6Fhg}M}3k@*;t>duCMD~0|gPU?s9w>qOx&gtsKN`gq&Q2Z8y8&v^qKgagI"
    "c0tq8_vmxs3D7qBFhTKLG^Gn1I=jRjwi$kYk9Yd}v_XWc@fU1kR&7blfo*I}Q8eg<r#-R*6^o2){A(xjCattCXxEDgvwCiYzIW}<"
    "!kOxaa!=>Sk(tv<eBJc|a(7LHz$kJ|@A@)A7Ddo%<Qj&jq^{A*WtW+@2?qAo62=58Co358JzoBu^oax~e+&Kj+@V?2xbowy(KB9R"
    "ipyFjAW!WRr~ltoG~5Iba4-swiZ(SxPSHCm02(_eIQ@>~`7K6J`cn|hPc?MXx=4MLIS|6b>f&*SF?Ei}c|ID@3l-g>RjO3qwx13!"
    "#2M_j#)Sx)Iiwp?@avv@54vhai2lmCkMy61ub22ZV%Jp^p{AgU-En1w5uB}@LsHDUwKDx4MI?<IR}9&PXd=qgSxh8c#(PWJEaoYY"
    "&N2#DU&Ae&<f&RtIB5hotc}r6{awAjoQ;^L$%y-OMRdW6{hU93bM9iz&pj`Qa^HgPaj4EhNA}wAz^{H6gpfhF=c)20HUU|hxNtI?"
    "V>E0m*4CCsv8!QsCzR5C(?78|92Z<ON<_0lQ>9c!{iI*`SbvXqx--jTS$VzfDQ^phL-14o?Y0MhWrr45&UC%XFGtKEdo;*yEvd?{"
    "Z`=wrEySA`k{&pwi5&OUEBP-cj?jLZ<4rM4N%pny5TGlH-Pc)qcQS%gHo)|nvPTgeXvimHTo>r$M=okvDyF>tt4Kh>aWSn1%=Tkf"
    "DlTI0F<-_zt?`OP*4$jSk&=ClDXXdZ8fvQhy%hVXs$3s9!5zmBbFHYT?sVH@V|2A>6LvfIfQWrx%N^?BPSyp5{;P_Tuczo&s4Ing"
    "bwPc%U#2YfD)gq$zrZ>o_xeA4{%5`9{ut&Ov9Lyu0m2xQE-I>ZNLa>?BuXHXEDAjJfktb;iFiFnJ>{P!rSI`hbz`C9k~1Ohze*L5"
    "W&k?r+OTW7j-Y#+)N46`(y3ivxFy&Vg=+wUf(1uc-{bud0jeVDT3v9o|46qkKN&`Qh8_fMBg#(pE<V^^x-!r9$q7DZTgIXiJ#~XO"
    "l~}?|vvMRUNO>7Aor)GwYlscv8$vI&V}{HftTr(6fDsq)N3m2a9~QYyFI)7(nAIrI-TJ!4d|!^&@{cB!ufbjS^=SpCbM1~H!YKxH"
    "na<dJyPpllzOb2NKZEw%HY+NOR=sz@B&465mYmfh?4s98Ky@bPW*=hICicb+t!AuI@ln#Z&7a15UoomJ&s`BWzs@#v0r8I8hJrYx"
    "GbeX1i#uQ3D6vCn7W5(tsPt;ba)PVPT*iBEK2St(x2kXv+&7sJdAV@AJ>XAcG2KS#`i9>7QaqxPedH>+VG-Nq2##)Ax9ZAMzu4tb"
    "Iy%t4=`|S5{17#2G0tJOFum)6R;BOp-g|-?0=O5&>l}rvMN70owp+W88FnppijkG4IuR%*g6fdb3|63R*Pe&#4mtSm;r<b@Mrh}T"
    "V)el5yn!spRV^x<vNfwEo-)_(h)Hk~hdx2@R#+5)=sdYbQq)p9R*0Nk{~ZHzOObwYEeu=xBM){N?-aBoOc%^!h$sk0g?en<u!uKm"
    "0y~2LLu6sOr=!qmY{iX`avmvry)_5<7peUo@7*A%3;wA@4D<muY`;35%gxY+M@LBFHjf5#ZZ)&w6ME>dCBG&|zPsc6?Y_r5;-Bc+"
    "K25F>IsMji?71j}hCq!{#UZVA(<hazLc0Mktc?Mq*vb}LntKU|%<sl-jrX3(rpT%1ZNHonyoe#PzaGnnQ$weyq@<ej{eg5O)X}ZK"
    "UA={411`{kMWeqy@N4$Ycv^8Z5#rN|uQfapo+d45RVmlhz?qQSgnMmyl*#;zT5W@#7%}(Z)3?z=II4kKh5{BGk>;8xV}4Ab(e8Ck"
    "F-A#Lt0}uR8C{E8`$pMy&*wC{HASD$_1)^X^bl?`zhiTop7YDc@L~@i_7XwVf9}+|_?(s^i3q#4I)(`yqfmt<1eO);dm5(JZ*>pF"
    "*mp<jvos)SyrK*eg$;zYVeb*@yDK6=1~u8P1<F0s2->AZ;mmiBoZ&Qe>HlAs@!k~TP5KkDzdxV<;Z)9C&4iF6d|8{LipyVP;Au3E"
    "1{XlRV9iB0yHZL8LhBTd>2p>jYV1dMnVya<oU}b<ldebVK@?bu-u<_uGrEGu^AOW{RK>f}1tlaL$O00C4zz+(rk1dEM5^tGh%)*%"
    "OdfqLv(~I*8<P}3FXJ6YZR9;@o<myRjWq+7;cXQ~nk@txP;Jj?V&ZAh{8a4abZZI5?&dTqD#0OMHcUAnf!fS^eZ!~X2g}byNOI78"
    "^nkFYfVlhiIH<WjJq8+ye!5hfKD7|m-2*FbTee+o$NYKL7*16n>M}X9pwb*$?~|F>O$fp6C9b@YZP2@4vC*B)`FY>Am7at+w`eMb"
    "k~l58%XsfOhb=<59A8t-=$fEI%X-%Z%h@yvr9u-SPJ?6^CYJ`&ShG9VOE~m&zR>=@rwgZwk#>SaeN*Aoe`<ZLj$M`Fl2GQH`su>k"
    "LKRxSTIb5*j>ZN3<TY^nDQ%|R>jk4Yf9N$*x73h^1nk=SKiWZkDNN&Ytz?WQWN}(y#th%NmGRWi>}9wI+%p=l2$C-2of{#m|IBob"
    "t{*7Efw6<$lp-a1K_OCeHT5Mr<&Ju<sX2@j;v{&t`M8RP{_SL?^={6=ssfO`xvgOW_omF4sMs;r2M!3U&v`()K5!J0qPLanhOXp*"
    "!l_+WFB*fAs*D|!^nhrBaq|&`McOet4yutl5);j7dwX5MuLYvEs^P5iItA&ti6cl{!u1!uWIBPy9y6mcny0XPuty#7Pe5I$(`x-s"
    "WpBmcYVAZm)O7uZ-L)E)546mqG=3THEy+rld?I9FMx?}XS~mjqy;WJXiYKhlgu+}Zc4e(%{ma&qXl<Fzl}Al8*}UEO9`6*kyT`2R"
    "(d@#`HX-(u1h&OcCUUncy8ZklRIE0=*|X8%jxCHnIq(Xf&VPFc1#f-_MiJBJ|Nh5o*;iw=onupM-!utUZ4CwZ*aajLN@WW9es$FD"
    "DkfGqX-rXd8~Zn|KShXv`w<69)ME1@wRKNzYSe>--d7YCrtNd;uL}4nSFf2H*G7WA;$ua?!`Qjo_dT-43+FH?VrX|+U;ap)FpK&U"
    "_WHo7nL@?C-Um3|E+z{1YXOTy(TCb-C0;wyhfdM3=Lk<xeIj}^kQo^wgAV2YGy{@VqN>GyJFjcJBg$4h({}%Y#~ey<@iD3p{@O0L"
    "Bz_Vx1*m)b%3)UAsKjSCv@#VYv=+naPEnil=TVfTRun4u9`CqiVVq#>71sMB=bcePyy~z8{~jpTY{6~rI>HHK2o#%glRpu^KX*b("
    "r5=%}S)cn%#nXl@2T;(FKap0oR0M7>Y$6n8X@}SaGbqZ<MP@zd_<OwfbYCcl*122aIAxACWE!xpzvtZ9Ve|ErT?#x3P=uYM8FSF="
    "uHC*U?MpZ$&7v^eToO(R!hhm19HdYIXBC)CyF`W4-b*?ABO-vQu*<XCG75y@@h%hvPHkF|VgVr}d;I45T0t>IJOQnc=j>kpEM3NX"
    "@6<#t6wGf6BD_%{Q(zNIzJC6cl^Vd6l9UtK5}6dtu*RX)FWA$CQ9>8x$5d+3pj{h!OwG}ouqoq3^>N$p_9@(i&&>)N@Wd3Zp@vq|"
    "?!8Snl?gRWQc~)ogBUtw^rKsWj8fKBkq|+5cg`*d_C4Nv2|&Rkfcmzc!pp?Pl&EE_*U!HdYl>O<+AyfkpB_mB2g<FYOAj1Bq)a)|"
    "?UIw)!8dQwX~>R<feu=aomH-y;_Mz{%V064$lW-Br!eL&5VeMOV#48dq?luAW?sfCuX{K5pW*ub@<07CA^i_i_R0rEnqjEcygu;m"
    "8I-RvYrQ-W&qZ>HQGxLqb&iF5$fZ)cj$;XBEEH<=T(tS7TMg?AZ3zXbj!z;7e<O-OG+d)axrD*C8TD}(yo^_WJv&)}>o&>pIOr5A"
    "=()JCddo?SU#V|VFG-<IEPGihJ`%f~YOzdfL5|`TLZCG%DfX}s@XFugz4D#{u*p8fHEz*#x#_P#-?j8O!%`SArF*|}oXH6mXg5QO"
    "sokt}@(W4{Ba4^T%XsIOS#K=gPuPy-c}~LO?sj3S&!6hxjN0j~oH(4qM&!H&^6_xbD+ND>P+=7O5lgeKQwQp)lT@@!6XO8ApFHuU"
    "4Nb%@j{;IcFZ43rDUJcI+nb0#MCqL}JhD@{uAx)YrZ9>^(QV%yiBlAU#Yf6@8`U3rp*w?YfHDN`cc}rdx6q`ZCVB~&DHcT~${Lqg"
    "f5hBT`;1F;aSmnun7`c(!!hNqS-ad_uO8A*2Kl8pHZ<{n%zZ`bgz<9}eR0%}D&rSC3ZIj#!kMg?Jl3GWPzfx4wN@*a@s6w*Wvuk#"
    "#+hdpm~|a_h)~)hLVJYs677IOu`}bz-G$dQ=I&`@R;rD@;lr2l%5k)g*a`meHR^hQ%a%n$YThkV0MR)6P0j8Sf%=2=;Tz@XoH8kM"
    ";FFOr;OP{W(#Ok*^KA*H_o&`+O;gEz33n}S^p(pe{z}6Qfve)1W<H6F>?o=!-rHmIS{=S=<8^K;7h2)_GM@V>j?&B?uV3&z%2gtx"
    "gxp^rc#nEewea!gA*7J~%8%Pps7JYRh$HKoIr>dk3UOSrh4%ELuPUotKfI8Kvh;HOLhoUykS|4INEiv^aqyBv4yRc{%6d6dFHRA0"
    ")|JW{TF;H#<&mX&hzA%x>~4~~4EO8npd*f2;Z9SQAa`6gwv0d3D)joJ7t(8(Ao0mTp(%>W$+WICZu@WzP3rG+kbK0|9j%V`x~Q``"
    "hshCn?%NB>@4<kwP!^P<T{D+Gg$5N3l)aO}<1qN8w?b`M5sF#Wq%j2<F{l?yT*4yLhpZrE^W_J<U;|BHgqHsMpUMrT$o`tzU(_<q"
    "B{cr@$E}71wbu0-8ogEF_PApcgiPDaN(}Aa*df2?5~vp2t7dIDrrNYP)W<T_+M$)3u)I@1EL+HRw}`1XH>$OdI?w=4!#~9<neRFD"
    "Pc?hC?91gwK1Y+nUCG3ojYIfV{&Fx`)2%8`uQZ-sK#S<Z?enM0;}LG={?2W0{nG{%Ma<CTFG0Cy3uu)ZA7#~@3ot}W03nX*mDKxc"
    "3X{bZe;LpHl=DOr-Szt&VLIyX$z#0<?NE>}EWFxqMCkg+A<U*fsfFCQ%N7^u0Z|^anhlTFrbi_Gdj%D9j5ZRztf-cW(X_r%d!#)j"
    "%)Ccv_#YG{&}?=|n>;Rly<#&1t?;zm3X?wf^qnS(JDS14*k}GjPgpN=(xt!IvsE0MwcCQEOC6;e;eZm?4vKAb)r=}8t*|tz*6Za$"
    "vmBDn=~d3xQ<P(hO?R^>Ixf}G=EU`n!Dl=OWc%V*o8AsTp3;XMmBx{k5qGH+M<_rFhALWOAn`rk$(wkzKVp%&M=^Tt=PF|L)~AkJ"
    "mSRX%#P2Z*^3u%MMSm2@uyYq3EtmixX!X4%OE|xeM!9R{T{v~GR%?zH(0YB~D0)%!$_(Q4*iGJoF@9{(C~YMlB6bj4KR~Qn$Y|oI"
    "ioB*?cYG8q#v^@JziXKV9nDYqFxPTD{47UQSE$3Kt5u*L`1^~r2b6ej1)AZ=U(zs7x$rIQ9eGHrcvIX}-8>wtXKut%?Y*I``rcdR"
    "!tv;p^wsc>2Lg4nblHqO?4+&)oqm1Q4a!iIC}I}4rlMkF#3hFF+in_iD4m}(ggTPHjQ7@k(7ygicqKY^?c-5mji8CMD2OXy^__(}"
    "M3M^*!^+#;psP`Q=%UJKM~gOos~Lt<u$pYr+~ixqYN#&@RwAQ?;7aLg{ISf6%ffYp)2S6H=HMj7B}^ZBUnmRgg)zOz%dmUn-P^XK"
    "4HNsrJ%*Bt4RtHY+O&pS3Rd|t-qF^`&;Qeu>o2aYy;sO4Mw95%P~@G~E<AxNGQcT`ARsm+1X%TdY0y;S8S`8GLl2)me}^>nl|$CI"
    "G(zaXqltN4wDlZWA|wds=>uiR47+QKtGbLP$0)gJE+6}<Ehwv~RG;z_CAx$`iV=q1%GQ@fd*Hd2Ik$J0KoU<0DJPiaQS;W+dX8^<"
    "#Mz@+8q$QY6r)7X5Z@446&f6h`(wf&r;@MO=}LaO@Qpr{d){4)pJxDHV<DKbThY=h1{GK05~EkJrJ{%ClaWLtDl$QwFJ<kvp=6tI"
    "A>H9J-l=#^l+F3;gxI4A#*~<Pec(h&@gX6Q+nKH&IN8W@_8!XZfm7o~lc>9P%|;V{pc+B%TX}svmeiIOT{=T<54;)9J^6uP%o#)U"
    "Gi$>Onl)i3;EfThuol-j@rmehmds)=;>vBbRS2PbE!?QS$2(H5m{gP$uTyX|EYhPc8=oeQpb*i}R9_<-E#ro4SVDNk(E*<OR4l6n"
    "AgwRI3LQSK>+7Kr@IBtqEh^|rP_!cyImAJ86J^1xwTwRvtXW&d%`W6_r=J!p%h8`ENkr}({gMg*--pN=@3@c9;&Q#d6be7aWZKSP"
    "fZGEv?Bw#vUu!Djuhi2a@q4?$MoHvA60zubI|fJ3?x%85!($Ba{lFWolR<QzwijXKK*2eU6lFQ2l)LRU#DOcu>O;X8#dR?itRU$T"
    "buD-6d+rFzT*iA(#IPT^9|puDpXX^!e#kjm;s}7XJZQKoURl;`N{}~Y2iluM|M5AB&r3^7AfQk{;o(1G^xxyvpA($ccex49#bfS^"
    "@JY<^8VsE~TCv51c<Y9b?pMa}sFK#}=TBLIw*=hVDm#jAg=_BT6!!b`@2aHR#6LfvBUpkc?cnG<;2(e;jhWa()w1;(_qGloxYkHL"
    "Pg4_ja<p}ZF)!o2)wCkf-r0x!W5FuM$%zN8tD2T+lmSyWYn1UEizKlzCFmNLS)xH3g~B?$K2g8NJ0?V2A1bQd!hkD3{Q!(|a1Njf"
    "e)!h5(8|P%VN;r;%ppc>+D7<Tzrk%mq$0H5|5#*&p&b@W{~qqWcHo*NIiiRHa8{&@;=Y{*)-YY{tp&jXJKRvd^0@uxuxI3$#Frd5"
    "XXt%CSM!(g-V@nJ;E)a)EyFU6h?o&TQBHGaI5U^s6ke_@RN1Z8LPtnZoVRL2CJHbyNt~DB$9UA>EOLr^(YN{kRZ2a%{Hak`t_$rJ"
    "Wzd>2Ki1L)m5XYJZY|5tCS?`psy^qtzO+}^qaJt%|9$+TZS`h$Vt#b_`UUOrvJhDoYPxMPf6U%OtaQ5GqL4plgb@hnZim|O_!&o!"
    "L^^lOQ07WTaKZa3=xW_;Y0y75THKA+%jQ~XcFY|y*--o!XA}w9w`TZw^uiF?7oxsS*;{Q#^_#-Jzs|s0WR7bo1_js8zsX*Bnf^!0"
    "6}uN~7xo(MfsxiP<CJT(?x*8!Xe@0f!R{32s>*iS#OyAm$9|C{7kNJzSd)Rp-R%pWdkWly1KsT0`Dnjdm#4=Xn_Qd{)(6}P4aNGQ"
    "sipFta$}tOx?-@RLB*y0`xso~9s3VcLV1qeg-IdZ$<c-bm6*k7h#=~^wZAd9QtV3FLoU1F@oDx}=+V$JW&LHm(^#gksr+$EoWcA|"
    "Ora6cUmqy1tR#goSF922SwfskhwCB@$F#^G0pPB=Jp6PK!I$agf)juBL-f4_F1-gvbta)RU{J3Y??qxaiO>s{2`DFST!hc){6=5I"
    "geT4?#OcZfGymhrjPGc6*ExIR2=d`pWc)g3M+*nxQ)JGxYQv(s(b+C&AMr|OYc-CqXIm}i*|vN1a7FQ{H;5e30q)ZNZt>M{Z?~Vp"
    "WcFqf8ISB6elO6J>*w9^CZ_o$UMDOacM1_GlkX3_fsNJ{#A*KfpQIVfqj<Nc^&9h*wQN&heZm5Q9m+BmD>nVRE+oG&gS3l|)|JaQ"
    "0uC?YMgqemYUTC6N)*QLWhK3a@t}wnDal_^iCvNLxoyyznWDIR(s`GaF9K2OLJ30siJ!*wsHAl6zEhhtcjD#hb}XKoZj6^=np&3F"
    "m+f5%)S3-CLlH&Qm%w@FbG-NuTF@&4uZv-81JO`unJ@gnI0+F(2m&y+J|gNU&lwzAC}GM7ESWs#%=5d7HpbK78#JSaa{!kVQ_I!@"
    "?il?js$0eC97@tUADM!7#CMW4eU_3%$akiV#+r;o&$x^iPyb@df0Pvk%}fC>53{?C*xG0=i{^RL`KC#<XfahkwMr=5nnjGx=}m9p"
    "ym%S!$m-zc8?G!kx}SK2e|o!&mR7ZG3Lus~kYb;Rp=2RbCz5A2C1Yb6zfngWb9qdkSOvpl=X4qF_{|v9%QXK@Kd-JK)UxAP`uPo}"
    "TOadJh1Z1x3g-n)Y0L?_#}%OgsSjnp)xY6u^ydIngev+gpE{CsREa#LP1gtNDOA$$@i$Ay7+*%CAnhOhkOZ>M$+lr=cZ=qcxvyd;"
    ")rGtibil*e%#PJLdz<v%Z4+%nlIU}|^gA&9({cLW|5!7UW@c^FW+_d;grtupj<$yq*Pf{vl$GZYzL>E>-9;!{r+wrCt^N_1Ll-CB"
    "di>+5I}+Nh<$>c(6oDwvR>f6Cxe1xoNXzGPn9vAIImMUCY19hOb!Xs<!ZCyU&Z^1%PQGw^gTeZ`Z9?a%x+PHEl>+zNA}mGmrw45T"
    "MYbjy7ne^<v0{wu=ywvuP2ZN97^)omb>Zi$drSP)0g=TGV0$;eXQ8Q%2D+tJhw(Grk&S~Q76<YhT^mD8prhil1LrQ<EOEzq8L#~M"
    "tKjdq6@03K#2xe<pex>YJbV?J$I}xTkv&%ubYX;{6o@J`)+9J<DefwuByC3p-o~o?uG_|q_1wJaB_*`X^fw2G8N+7*l5TtBkD~^u"
    "%~LC(Jixc<Bj|c=iRlUx<2HTKRE)oxBno(hY;SBQ)t}uCYBIMH3M@wsPSJPMg7CwwDI!<92%RFq>f-OUaZjz8qgY{QS2t#7dgE;f"
    "YEkE}4MBrJub1J%as9;;N$<A?I;~dGQm^`RqxF#*4%LbRct8akT?oacv=n4Clfx#{NSLBKT7{SK-Xj=vRbshL#}VVpnHI_`8#_kJ"
    "QLNBA*ii{-m8Shz=;|F*7T`1ATPq3>m}}dv`Rb;NCxMjES>K9L3fjY64qfsqNTfN>l*nAaU^sGrP_J@5GB{;Ejw}E0C>(!3a3sb#"
    "kofabFAv1is}j=VB+<9v=up(f!6&y3DbfF4inr78uU6OY>dU01gzUAoQ?sEv^troo{`YvVdQC+(<6*GhpD(8`oimeoy#vZGMb_)#"
    "q$H6yd{|SkQJgppp>Sae(fi>QCBllB&f1{Gj@b+f);r8T-Sj=SwpREKlf|4<9fW-Hm5`PfYw8z8>R!_IfpgJ@nflezJ^m!(Acud{"
    "TJ$qFQs<fx<VBK?yX^Ly+zCQkj6H=ol`8nr1mf+4q&*sOSnyoNd%+R9WD%83>JF#y@Z4r|=#gkbiYa&M{ue5a>FN8>&gAsgjNV<&"
    "Sy)0lcYSLj(uwz8=_m(;$5`2}Hmbtq)-ZEvV-K3AT;J0ue3+uRSuiV64iW(uh;du|MR`-P`MaV4bDp=?L(}z~2Q+awFK!hwE~m4%"
    "6654dW^W-=Opc@0P@v(waT{I?zR$u|1!IqI9eIs+M8fcLx>ncffq2ktvCrMGmV8R1dnyQkj+!RT^qi)NKj5%}^ABp20*_$>phx9Q"
    "Ub!)G^!hzs{KY+kRRD^DEh3SCu7R5_qR^jc-(61o4!K6Wt&!*0V%KYRyyC=<DH5IIy8=4?e2lSPdAuJ*G?ow8wc8vYKM*WPb5a-z"
    "_9?ty96sPg7;796UvmA*Qlk*9>6hPk>P3WPGJV6h7x1<@2b6;oMMlN6hD!KTQTJR8Dz)?pKdPFW$6tC1v08CQGsL3W6qoM4qSPnQ"
    "bQ$kwIj1dd>@^~!Q!?||hu-Re*`?>YBoQxepMS69YJr5=>ykYOnGSpRm~LOFzI>%(Ay%(vUiJ8YYCYKMuY|3>VY%Q060gVqM~$!q"
    "VXkmZWcAcbI>f>LhX>v=e%g^Lr@Pk&Uc3}2S-7K%dXzy$YLyzyV@p;9CJTy5;@{dl6E?X+uTJqbKR&qhyyfLbZV7o1B)H~Yg>L%G"
    "crVtl$zxzGfJ+Oy60z9*R3c6VYKN6p5$7~ZFbzFhuU(wQ5fn*~B4{Hcx9`h$pRYFl5qyAZT%8t`xn2&)sYfQW&`h$O;dH*n-DZCb"
    "4v0CZsZJ^?yJDJCmV_7Q;hTeTcGtTXtGkSMF0xVF4%cFaditf%l`2;aaE#X9lgx^5?sc1$BfYP<mnq21?J=%$9JW!{vFb_)<pyOu"
    "t(l<^iQ9u12MquM!U1J&foD#^TD>fZmm?7%L%#3Qqb&$>5`wywjHaJ4FA78mwO${1>v*}F4+Mg3#+>;{|I<gTBUcQZ<L8A1)$Vg4"
    "toG>lb{m?V+CXvVSO$A1e2;f*m8c9WoC9S2G5a{FiI-WEr_(<v>uC{BV~C<SKa-tWgr(cXsheIGH^;V(^{wZq@s6!h_+!2oOVclC"
    "T(;8GVoT?`&2zq3C(2!!8%L9Ib_+$+pf#*BtI-b^ZJ*!6y(LzPc_<KHr~l2RtX(W81upzXbRGqF9|9H5t6L-o5mnG^)Q#nX$H3lK"
    "q(B!9y+dq&MqJP@<JD8g?*9mSd+)dYd}u;X(THlNSs(3&jj&5MXf^eMnqg^yuJp({*+B?s``It!z2XBxi$$Qs+~(sYOj?QTy7IVg"
    "0jY8s$?AKIYLGz`9#t$#^f?MNY8r&LlL(9uc<1<TdaTF%*#3LGQyYpIox;1jeZluGB6F8z%=HJoC2d|4Mrq=82iGI?liVZFa(TeM"
    "#R*~<G2=sgd*D4D&skBz-vkYkJxat>4L)E*DWGdFI<%7u`gCchy}TBr!h+&OLJvHrh`iar%PS)7-;gvbK-SB6<#ekB#`y1llx&|u"
    "ex;haoAznTYl&OfYO}sW46arD2tr)K_HIYx^jW$?la_D3j2C{tPY^TQ!m=KyzeQs+H+Wmf-0PpOA!?BZ7UxFaKyRZDJK3s`*|7;b"
    "Gjj_yAD!{Xg;(I?d%UAf$i5e%x_-e^8Jf^=bvKCv3L4(}kx2g`UTqG)t&TQ}vK}o?1~)jx%q+s0vM|)fg<PB_o81&Zy%Qn`E)6ey"
    "+`h-lN3RdOO_wcqdsS>*GqO$V;&3A?11Y%m(Pv#+V5J)_rjmh9tb|^Q0F=g?Ykv>-mP2ux_Xts6=EEI10+yp-g1<g+WZ?1Jw6|N`"
    "<bExMg<c9zZ37|Ana*W|RT|K1T>9^OLCf~g4@S~Ru@Hy&Y?XyY1e?*46VqDuGG6-qqWb3?;qt&sqV(<Jz05v2cWRIn8zW)DFRIa<"
    "$YJZnTc-r(w0re37iL#UY1sP}D57r}Va4cb=O`nGrAO0cykoPBVCT(jLjlPjX>EO1!X9>aqes!_vPY(PXxCk*nEb9rh)zk*Yl)X8"
    "{P76c*lmOgwu!TZ_FdIa06d;SXguEZ3zCcf_}nVNaV=Q8W~w1MzR;HzQS%esXxw5`c1@X?f>LcBI`UhO!sqM>+_!eVEQuG7R|?i#"
    "ysn6R#Frdp6`JtryP|RxS+RPvoMA^%BZr8PwrmN3+iKcfxw}mQD4M~cV2;OgAGZn2X96oC#L8>RKaW~N>LE3B4RPfjRB{U5#U-@P"
    "5lKiRb^&+Id(X{+jpEEr2>yG#_wY5MGW%!^G}4>sP!UrbY4KLX4*^3Q|8!xJKEhshStJ!*UUS&tdl6Gw$Q^!<cPtBN!u%gH{m*9C"
    "R$;(L^MsrE0U)Qskw&WJ5!ITCA8ls=WudendaKn%+DVl_JG4A#+WXd((RgQp6z!N>Em1n2tSDecceS=i$CQ<}4MDkEgMc3RDS6Z8"
    "aXT0R(jEQ&RG#y3akgBR@!C<yB|L?fTl~r^PwTZ~@;0a~(bg01rUPn)rnjlZOf_5m9`77uSZ!zQHg8|>s1Z0$yVEss_~V9cNi^4c"
    "w&Z_Z7|@AuyI4x$r#h#NP@aOQC31p$#?rj|=K@o6!r?$r49_m_ffJF4pyK%kbkUuWDi!?r6kW!PC$7};d_TX?vBN!Apw=xf8iE!^"
    "RW1tDTv>|zLmZwqcYHgdUJM$-WmDh#ojAg|xeeOb4-0onP_?s{=`)~gUN|%9%{L{4sTdrd+OMD}&OKRH%B~8vu{{p9$y~-eab3L9"
    "xTkRP`-wLba01}cBSyX4q%m5^Tc}KMxR6$+5fJi1Vj|7TNv;@y-@ZHT&(*g&xkD*4eed(|26oyzv?q}c5)+@OiHVe3LFx`G)=S;`"
    "(H-B|*u_I>IL)s5mBb9i;VB-+*8aP$lS+^Gqza&XEoq}R=?A!<!rJQtZ(l3@iBDx~LIY8YbX#renDR0^Q`tArv+-tW?a%|z`_56w"
    "R*+afR2U0CE}Osa`Eb}heL9}zr5i1b$MXR9?|eH7OQ&RrliPq|_4R>cX-~xGdOhc%y@a)-eopl}Ou_7^<*QZY=&4DCZgK52<O!P4"
    "zg0H(2#(V&@tw?$FQ;c0KgyB5g6e8oTM2rBS{&x?;DwdxHAlQ|QXSV6ZxP0CdulHDQcDZ^d%X8_6$>!!0(?k-M1V9O0|IqopV|tF"
    "@~*L`MJ4*8e~Y7(99ry{LIs6U8%Pb8@lLgP#OLGYfslSSptwAlTCXeO6=nN#>`d;orJ0_ovTNutv6%&!m4;&Tg3R3^``;98Jv`Ux"
    "PdxWPX4Dzr;g|7_svS0<;d%@ZPgg9WkbmTDFAt=X3$-Qv?OG-Mil9=WZzWtGIDM&ly&Tow5K4OC06q%#fA~f})#XHa+U*1y7r$S#"
    "R<>82_>_(j0B4_`T1-WG*V#2T&Y{ejcW#&fh#BQ7%7Rt;2*NS~yW&-06$LLR6dHwBdUKcY(h-~z7;rbyGVF|AP-k*{;D&Nhpu3>#"
    "q)n_fNu7D7E|s>MO+~NeNU<~o)eY8#5hq;kzEMN@9`D@;f#O%LJa1p{y}T2zOOX<V+XF|pp(wWs&D>T^Hvgob0$x~t&Zr35F&GkR"
    "jQ-EHnBIqWM-Q~)D%~i4TCvE^(aAv3!;Mh|9KMgpv;8dzOq~=65JXBt(Q7p8XBF>2O9UyCAyAe?M}+K`K-xwdco-sw_-yra&ABea"
    "omOZ{bL~}?j#7sAj5}Ge5PSW6{iy|M-_cGVT{A{$RYP_(gc??OL2JIn8iO$${x`2Y9<h}TxH^e%bwq^e>-rw=-L#xFYS%^tc9q+d"
    "S9Wp%$Ec}wIRqAGdus5Ym6VC&%CKA;5hHI!R-4hnao+_R6s^A6L3&a78dUNTOh884JvxQxrv(g1>l&SKyyM!K{49uE>WRJ!TMAa>"
    "UfY8AI#;H!N^JY{)=4k)9S;Gig#1Agtl8SQN)tY})lnnrG@zN%TI1$xL#9n=6j&(N;wg^+3Ags~yV5S5f*iflDmZ_u%1WmMM2u9w"
    "v0mX_c@ISCD?y@8zo_A=DJCMKvSAKkiw5_Jb>2$TaTg|r;An?*PCH~j3fd##7V5usvD1;F(b{xaA>Y1ViifBR7`bqrfzw;2Z->Er"
    "xjv9yI8ue~9Oy3XEtn!qwUIqU0H!xY+xjCkvd{`FsaXn#DBwYu0|SFW*H*l|(i(oJhflN1?fbpCpIhJf6kIE<b=S38*K;JJuUL@H"
    "(dI9Wt3bLO*Vw|0v5k%Y%<w~7VhFZ*QNPQ0@0|ukaQRj_NHRb7U{qD>`<bpii15@@PH0w1cTgEUE55cxj3A@cMF~A}4x2kUAy(~I"
    "^z-%KzsIx3F*859<+l~_E6{k%6>jwt`uy*-k-Ps}9~U(i?;CQaJjb=7dq~KOrLI}+^t3rZH6po3OJpfMP=7N8T7p=<NGYEkV=Jlk"
    "wl3q<)1z(5qj6#t@<bdfjNA@^+#>Gk(N0l{iwH}i89mab{Gtl6FHSi(NrN)@Z=mxtpG5JN!o}N!ylKs1xFzO&g9S(X$5H5s2HNd`"
    "m)2vliYdqK^KTfzt&B?@B{H<ExEJ0%dZ_)dL&Qe>dngt<;R1|=nt%$YR!MY90lhHg!L&=)Li#EBaFrD(`BtjjJz>VpMLf4-RX-Jn"
    "6mhH6EiyMEBBN?J<)saN6I4;e^{$J%e2;e;UiGrf)7jUo$(x!y0b3!<jk{wVy$RpCCHNG}=&O$5rV?`_^jpO-En8vB-RJj#a($5^"
    "{M6oY-_HxOARm>|braUp)gt_duxPPcVNW@CP<bt8x`IJV2v2vxX+@mu=2jpcnVXQ2ylWZ#f!0R1-+et4iND>XCYO3MvC8I@*6Qn3"
    "%p7kIyoZze+W-}F`}|S+Dn6UXe%cf+5|{HZ7IC!yW9-dxHR+M&%q?*qmctQhK+;XO_Ah&QD)|0@8cEefeN$%gL<H$>=5#k33(=u5"
    "nS-ExdXL6!fUu1j9#!nAtz!lG=JZiP_uuQ?i_wa`!_%*VV|N?Itv^BqyAG#=;u@A5MxcQa>!SQg1=t1clRArO%BQxqsPLyG)qE~J"
    "FlAoWzt>B@x=^MYF}{AlYa13K%Kz#4%D+7tC`%lnbpWi}wcA>^uS{zm&D9cyb{2y~JqbSIW<R(1q90y7O&CnbuBP_yx=S_}2?T$f"
    "NJ#7=0^&m`Tj-VIgsFdYRfGg#RGm_!k?lkwBd)^mml!0bW;BKVTX-?wW(BIRiE`%Sh1g4H5QHx7<*byiw_s3`;Qi&UU^INzjvZFy"
    "`}hQ(vbY$zqOc2omA%^5?qDQ^Yk`B~agdp_{(H6X)%y|36nT37c-LSY<>5#S5=6=wLl8@Il(3R^zt{~ui`tgd$sKR&?_#-Wi`b)~"
    "CqzH1pwxJ*_wB$85(m0QolY@VampG~niL#ttJC2AK)rP9rPKc1nNAb?MQzM1i`!*!E9`%-_l+wTyxE?;wD}slm7|ID>V-i5^^=tl"
    "SUJ)u7KggfM@U>Zb9KIpvf0*i7`#-GrX41A2DEe6Mx}0O@p2WlaQ`j^XLq&BgdS1ie4Su-qcZ(Ip}gdDFtPo0%nNzEdY1GXuCI|l"
    "A?Ee#{cYY2!M^(x^&7t8C|AiLJ%6C<%7b;l`$B{cmQSZrjMA9!d`Ib?8Ujvy`AO(E{FJr=<CFgR8@^Ohne!&s*<zD#qUci(I6Q(n"
    "{Egt(!l7A|rX;ICs1vf$pqZd;Z9oyCjhWBC*ZYR<#T_i(wTc6NdE=d#!fO;zP@sV0X_q#q!zyBH(^}OwGqLuZ3KUzvx=wTqV|%Rk"
    "Js(meF}(0I_dq{)15w^5TJT?8A}18Hp8NjihK1gUdCGI-ds|E&9h1z@6+X^@MXG&2vi~RpFFl+Vlw6EkRsY+FFQlF7F@{CBYby5k"
    "GP1zr2-VR%EY3bA6ZCGbjB?Sx0iO>)XKrz@XIoPKxmk%3`1<OR*RQ|V*F?f#p5O2rR!Kpx;7GmaJbiBhw+OpV(KiIi(PC}ry|$JY"
    "!Uq4TbSbO$U36H44T?+7?ebXfniN{+%2Tjb>wcR?XqHLIS1h|KJf7MA3IM|-)?=JG&>l*)<=t>LK)N>2fr1bOqKGYq-bI3D@!ApS"
    "PGm8gf1fk|>p&GiKF*r6YwJ`|pRRaP$1x}Lh}uN~X66SPmc+s9r@;8i2&f2)`c~)-D-TF|?W}yFt3AF71vz|ky4rHe(6E}s#h|dC"
    "5X_>;rp@1c&7I~P+rt<~tJaq(q++^5sUWA-F&Ccb3_jMof-j8VK9#2M=J*CXv<o>Cy|lXWCcq|PGkgpjSVqAa7giy7k!F-`6^I2-"
    "mqnrt^Ki4X-e~yua<?~7@T$<H{QCKR>G$l|*O>}m`laga<Ev9ozy7Cr?El2^g_FVCPVbVgp|oukWZP&1atC1+t@TmdGq$)%34Qjw"
    "Oj9f}eg0b>CI1Yx9D_%1-Wh&k`8wnAshpLsz>ENtQ;OsAdc`)Q0_iv!c)6`5Y(}JUGmbsPpzCV>$clui;3}{d_27u12$lYy81#Sq"
    "zx@V=c+z*#MsD4=S^La8>eVS^cx*q7o0!?=DuCMreq&mG=FaU@nP>|4mK#gx4?TbQ|M%bi*Z=7Mv;9B+T~A}K60CxOIhH)0zBp}8"
    "{oIiTEpZHe2|QDjIzr{B3v?lsi-|(m27edaI?&wIkEWG=EBya0=<fSwNEZc7ym#9Uy;;nH?HVyxX6L8d-cP_VTxUBgLbh{v`lsZ>"
    "E>Ng8o(o<}mBWLufn;*cSvZn1d#o#oTnm!uz>S(vfSy@L6rZMSv`>d#wn5sV=!1=BUn|~<JkkWTPlQ$)&757|&^gnDh&<J+u$#99"
    "Kj4DFt6}P+hGte&U`S?+S?ka?do6gib7_%b%Xh{6Jw>IXVsj32w=PJ7J$jp~rW7__;XyFZnqm9V9MDc$NcH5no*(fWVibz8$8oJv"
    "9^XWGi}|goV=35g90@39=qRjd%8auhl`z}L`hk`{w*I<;S+51N`>$%fI7a9plyJwkGYopai~d~Ow~n4xRVu0`^t$!%x#3ZS`%#Ts"
    "oD&(|lga<R;GOlzh!*Mzn^yofD>F*VfY}@(IE^DbX&C7#AQA8AZ$L#=LCqcs2mOl?!s`jInl4IWo+fa`#3>NH6QPd+nNC3hr0Wlz"
    "g>jI=Z!=63Wc&pLV>V?O0e9H+Yr#+Ws3yxOFVsDWT9h5drU&Er*6Rf7`wlm>3{13ze7sV0g7oo*M@+Wqd#+H{d%=$!D{atn;^vr?"
    "-S}YoYHg!#pq$(+wROL7#n2bs6YJ&@y&r3)C?H2baQ3xH?e$}&KlDs$jFza{C`zHEtnUdtQgjm_Qlz6#r3ewmf?)bidFbHGa)tw^"
    "09AV~_<@6>Kk(QIgdB%<1H&8m2K3eqmNLL0@nbtZP14rT6Re}7pO4Mbkq1Dj<@tpF#TA09HhO~fk@QZJcD@1aM5=+_=yeJ8EQk;4"
    ">zjL%h1jQCX)qZs@~zLkp9_Wyw`g<1O|@`Oi(Z2*p?jSGosnd7s8P@EDMk8I0`FRG<Rp@Te3r18-v08h1;2(1YHg=NnHTcPD=lH&"
    "ij*6k!=jqQBg%vXs!m%>bEBAo`v+dyoga|lp8ouX*VT?$*E195;tp{o#p5ivYiLdB7^_o*$Jmt?!x>x?xTdi|^b(FfyP-_n?0l{G"
    "QB%i_bfjS7la^DEJ(6Pr+=}G1)ze62P{Ovt)}m^3k+FeGY<eS6KUM-?^!JM4R-OF0;@4sGc1HBlR^k3RnnO4XzV<yh+)i~C_Uh(D"
    "P`&)pW@4;XnPc|ng7uD=i?yIcX@ToOOl!8z+(lNH5qvV4W+71V7Vj1)Si(3;CV~O%uEmgD_1nHuu={4zh3)+*Dy4}tpQVZx530}?"
    "N-4XoWGn`wbECW^dRUh0%Z<E51_Z?Q9uW08G6nx$FkjW>e4=SA6^|t|^@O?TjHC#tGAJ}F7jkhp6eNleh<4^)Nw`f9rlb|z@@I7M"
    "p9|ih9&MmXA9S0jzcZfg7kbfQQn1^~Cwbb0_0G~6!oaiV9uXEpizf^6b|p<8p9_W?PUDGgO(A0=ji>E6_0bGht%}SvIS+vkC8OpD"
    ">!o8l=C@IZ<I6I$t#kTX(A{7o{!VRKnIgiRau3Dr#CVC-%5FQw(Ong96a~u&+crizTTVj^U?<T*?V`M&3x0h8B9h0617iSTH4q9O"
    "7PHf|phQsOw0%sk<Sef1#zb}>fF5-$lS8(Lw%hbt@YBAF=$xHNvoQqNloM*4Q3<eB;WaePj-N>LYiIpZ;bnb;6-DB+YdT($(B7+g"
    "0Y!JG39Sz(%mMeS!0*P1RNPo;Fp8E$JuY^Xqts0UW}AwITd#g)1a!?};zcP<C+fLkyfY4>7&?eL1&n(L94e6M&zu7f5@e1s$eP@h"
    "ZcJWjwG{vwP^^qQdz|FX=gI$cniSelw^oY6xl11|`eMgu+K@F28h*w6w^9Te=)6$H-!6n9A{V!!Z&)-#C?>rY{K~(jVg`$PU)Fo-"
    "dkR>IuqjP!L<rCubv;=(t#VRl=t^-<Pp4Co1r(}jSVZct&jo*mf8Ib29lieAH27ib!`dJS7uWJBol;Ook%w!L=}vJnGXp!3d(k0n"
    "@x4vIdNpl>j+3vv<OSlLl7OcV7_8Ei_DHzghzF5xFJzpBp*9!?4veA`5=MH(&js&Ph@}C;GH6YmKyXuA-lMaZe1+@k6?TzQZCf2q"
    "RO?evlF%dLv5*zWS6MT*=R5u2eq?TdhM-X(PL_6}C}OQ29)&|Zk42P#1_Hpb>CgA5Z3IJCdlO!WyyhqPcHfLCZrTdUTt*9ADE$k#"
    "7ex;p21PENaU~+9NxLR|u@;d4T3R=)<VuC&-oiSA`t57Q@@ZXRb;ZyDfJ~3O6#RS7Q>R|Ly?F#AkNKtV4wG2r|K<s73Q2fi=8dP&"
    "+|ON8u2c|BMHGIBSY}7<jqMf1{nBw6Ug<FiZzxylIfA8&LN7e^&}qx233CiZiYJhA<(6N$R>Z5`=nIKQ%*mV&T71SfqYW9j<4=O5"
    "`m6J3fh*h!)=&+m93j~3|1|Xex!_gK3Fb_;i#BivYsaR#!z{S6MUM=YYai-Vi2rkIH~SWw4CpzK$4@CY+3{M>-{K>`%TZoNAZHEh"
    "bjT};wXR_@F?8w-E_uU?=s2`+-OO58XkceIxfjNWDZP$`|2U#3BE6HOi%$Gf0CNJ<%@iHLK|2hBxl7+s3tJqC^wO&W-kwp}tC`tz"
    "6HK?y1+O9ohYiJvj#BfoSuaR5SZ@h_ot1G>bvD8u-i<=5WI>)7Kgm^hx9Gk%p{>_~U#FQUHz*eiI1x;&g;wn=Ou8z9I59VY+jJD7"
    "3dSYg1>a~Gnbh6t)FDPmbA;!D<(Brs8D^(2yis~|iYQY*dX6jjJeqJXV}4%1Uuic4vvR|G*3cPgxYQnF=NF^7_cAn-pAtUij+0?j"
    "&lI~Lv0zj|ueG8&qhx6mT+$Ytn57NIJrrYNSo+z@Ou;=D{JGKdMIhb;%#4pocUr90o^|$hRuW!cyU>*-?B?~KjdF}m2dIh;F8meJ"
    "3k&-zDZpJrLHCc@^qiU75oB2UY(YoC3n7+4`=q1ktl{aIY3|HApH?9K=qu_cUFEsrSIa1Z^W5E8G#(Zel$@x8ZDpkBs*VV^o5BE7"
    "N1=7mjlyL~uN(V!W19BSe>~3qx!{+B<JpODw!MUP9OKraqfF*^L3#mWAxRx>?V1q_%h0OjXJ+2Pa?Q6=6{LCvoA8M#V(%H5#8`@6"
    "%109gnu$wdW2%-)oAQK*RG_L+a~o(6DX}<I=bKJ{QS|oax!_M^E0%MGY|O%6M0AFB&#t4+Hf+l8xD$0mENCm*6MM}ZFmVwlrXJ;J"
    "p?EEL2XvyMH?LH5!JZ1RNR1b}=%^C{!v(EZ&Ld8AHQ6Vd%Q__vbYo~ZYzbENTJR$@^c*{`PD*gtGDqyLMLrelpxionLA<Gm$cdGr"
    "41}Y*xSJ_VJsKrr<rNz8p9_||pgEqQK4%5CA!yMC3dw?3)zc{~4lazd`>DLs?uyYW-t`W4?9xy+>(A;*xZX(dlw%2Qr{{MwG@)0j"
    "bj5pHfvX(0g#eFonZjORR3}ng#N}JhZy6$c?$V$6vZ5EL#@WmW<*Z1}YV(Q5h<Hx@Rp&414-bvvlg@r$*h%X=>Oa=07;LGVJs13j"
    "H{GG`_%Y<&Tx6&vO;_>0M%mGBMHy|#?gXD!7XYV|S=f>LjMbI^$kGw+&nNt1n={8O8k7+*6%$BO1f%TBgb!B>Zc+9s0%w7_i=9O+"
    "w5j$U9KzKYdoS~qpnet&jO}sB&cz7;!yOwlrn;>#jkf|8dNgr!262n{la6K+jYRTeXKai0zW)Sn{=MKQ9*u$yGh9OlcyKEtNN7*%"
    "qYm~|2<dDOuh%?jTksyuf2UnaQ0!JPEDr7LXQa70Q-ynaEf{s8Dd+14clv{c&Xa&B2F=20;xLTca~m5(<ie!<CeEMHk7Zo+S&0t$"
    "j!fe>M60yLU?p@eD=J+uHqDG$QV?wv&=DJ@j>5315(?U5ic8v8I{gSsKX+-lCTR7@tT{A;F;vyTi=m;lkc$=U*WZN|l|qyRLs*oo"
    "w(yP4YslCcoV&L)pX+@lp(g3HTS}NwQE_pV9Y;!{?pa%dd>Cq?){XI$DE;<~>0v)5vPL%9lbrFw7@=Nqy!t0uxseHRW}<;oy^er("
    "yS9DOrX(>IMB7GHXmDJ$HEm!F$HaJ)W9C?|Jrl2X9l}oLlxlP!m-0D*Tk%GzUE8(el9AjJxDryt=oz(^GvA9c)La6KURkQY5XQBA"
    "sI+A=x77{SMFxSPl?Lt#u_{ZrxVDmdM*XOCdJ9BscGEL4Q5q2~$aGxp=YrSaDN^85!cQeCx*5TDy~;}ean=Vs*S|s@y%JY^ghdLS"
    "uA}#lgG_URJLXx8ir-DJbAjniBq#-P{~(KyMb}qifN)sh;UN~U^x>9c$kb1_6z9Z%^^|(~j1xLLo(tXvB1>lmwJ0l0qqNKYj)0or"
    "E%C%x9e0szrM;5kS}1R<v>Lo^97dkFtbG#1ctyrNj<;yU=*1-_zN+KuwWpHW=&tDeiS5s=<)(g^raYOIK=hLd090-1hIibva1y@;"
    "Ucrc9xI}bgYme6AsYo(O;}cyKm?+~#h0sWW!1O{(KYCL7nu@+c#2&|n*Mjl7r&xbVrh}djma?-7t!(`mNncm{F>D>3GX>T$ul7h="
    "mn7!273Xwr6eyIEp9@|aET<PkEVW%9$TMj#h~j)#+-d=dLs1pWM2^nA&ZwgLrkkG*`7(iUdgHD=ap@hFr`f|Xyd~h}^)|IT1fCkG"
    "eiTl3c1T2f+)wnvo9Gn<+7d9KSAG;wY{#$Ux!{d$7H*)}=6esGW{%ErJi6co2CUDO6$)`9&=Uz8Dis!5!OT84D`aKupyz^LKFrK4"
    "3e#|T?RUcc2>mMsZ=`+$bweg4M36fCEIXRsc^%C<ZC=k!C%&|=1-}8jGxQmd5K%@SbfDIOq*3NJq>EV#VKgB-;=md%30%x^`tt*h"
    "Ga1!SU*hw2CS9vCg;gf^x6{kw%Pl;boI;3=UTY6wTzak()9d<&h+uuN7`at1M34n0x3BiO-j@%(&TT$d7o9QlA|hSa);kkERs`Ra"
    "q=T|mTYzxnqD1ri%)PX-3uEohv=Ux>CS9jA#<7kJc+@MT1N&IktSMMwqY=T;yTfT=^J;@Ws7OD8LZQ$hF<OW?pr!YM;Y&{nDaTh`"
    "a7=)=Cjq9Gp}->`d=R6!TV4XYF4j5?$_R4}GrD3Q!^9@c?4RYtbe+>2(x?stMrfxJ&m8btQCh&20D#*y&bbT;V{{$#c)YUo*3sz6"
    "*`_?he&OJBrOvUrJr}`DZwnFvPqi1D=|$&12w!MV?GY8i^;52*Xde7_P1#A1ZVV(rk;;25_+75+tK<9&0Tx#g=ftul(<4<V{%)&d"
    "mgs-Qoqi}G36V$rXaqbnP~lMq4!hUqg7>O~S$;e%1-5Y)@~il@5D;JxDPstoMi8p30|8jU4;mI0DqgvzbE7Kk=$=pb-R2_#ZmF1}"
    "+NeKHpnlp+tK$rV8;PjXY1cG<Dowc&;Gas%I-Rv+A>yqEBRs$1SA)jG$kyY&&<f2-^u%H-87um&ejyD%5lP@$TBm=a$x_myC@+}N"
    "5Q7mec`f);V^B=-#}W})!CA16S^M<pq9$C~o+4@<w^i<?_2xxLh1*PP^ulC?IU(s1Uhnh@9t3V^=|<G1vErD1c>zv=F)H6yAYN&="
    "iE&uFPg`ohg;M!aiyEJmp+rL>g!Q%HRT1H6$1~n73WH{}srt_?3fFHhMy7PzMUyg~Th1zg`g$5T^>Gz&nP$tS3u=t#ia+1Td?Z&v"
    "Nfmx1BG87?k5YT@!sD)PbPBxKDt;vbSZ3}C#iE|FZN~>NzGBU_Zqd<l4%}u;5j-(jB)W-0myvTu^%eg>beJn0yE>X}k6rY*KJ9p}"
    "f)v`ldFH2lQH{=h@qZRz!bh9KplrX((wRoqObAgsiFWb0kgL=ZN|K?{IT*X=9)mOb-ZtN`A)QK={x)ofaX8a+hD?i>axOM|1Xb@M"
    ";%<wPXhE801S*Xc>=8yr4qppD-)XwSAu%6|tvuT>^Gt^x{WeMpdi+VMOm!gANZZcInn3`v>USwN&B$5x^SR*lP4EhhNZ0A}g@Rz#"
    "5S<annc^(7ERL$td2w89iRQFmMeSI}NAV23l39u%<a@!ZRf5VS?E@|>A$uCT(ngb3U^s7Dr?{XR6l+#qHDD;lMUk(Jg)9y$eLm&6"
    ";CG3bv93VrH()``cyr|{i;Re0ngz0mQ7wlTFG3xD>t*WibS^ArB>h}=z7j^U_o|y_`^5c|Z}?|-q>0D2235i#p32N{3$P9CR&j9D"
    "Jdor!LR8{9DOgJU^SR(13U*mZmdZ)YHXgnCsnqHnx1&fgFVE<Arc<;N%Y+Q|S}1I2aW_`%K_B$_5z9By=`2E<AgS7w^Mq6C4E8v9"
    ">hN=A;UZVudV%^0QeW+CBZk~#rx}kb!?`$mt$3ju>;^)ktsjyOwdlQ0gE8m&d|I*86q@jb-BW+3$TGH@5F9O2I=}`gG5>t2pV&bN"
    "fuUo?8b6fbHeRu7RA7XVA|oU;wI~D$x3yIEv1=Jv7wN}q!d9YDInVe?isfr_K@!*?h@u=Pa6Jn~D_Ek7rh&J!0FKUH=a}mF>dZGL"
    "9IJ@8rA#d#6`WTJeJ<#(la?(=gLWjx*d?NyOPtqE?p$Y_IvAUCAp8Nj$krP{bGmToTo&Bw0vz%AO0PVFq2rnPysTVn`=rdP!`$_R"
    "0yl>I%iT~7Xq~uDja|{(PW<6*RIWf@-<SHTw3n|+>+y-B9wi3IxPel=Syel*?jD3J)}+nORy&{rs0?C69_T$uJLu)Oz05Sv+me#+"
    "<+CO7uwvBv^ry*FTYg4sonji4*y`VGtSG1c$G~bwdt1SE=r~T=u=~5@R=#ob?77xM_;G2^B>~)_w5&g1u3iNILN_`X_%IGf?M;t&"
    "9HGKirV=V@*-U)YU&yAEdlxLcLiHq?UWGcN^CVh5qmZK_IeOm_*>8QTjWD>*fU7I`Vd-_Lj8aswlIKVKcyPiSBcB$DhIB*?%IKmL"
    "4nnVeq;y-AtGv==*3LA5#HF{Av}21lpH7kYIITYxtha-}qd`IzZk1{f`<#w5D_qaJ#=V}jYyv$E+OBrcBRy%1fb%eLy}~Nbp1P~#"
    "!T|eGim3}XGv(E}X<*5sqzjQn{m+Z&FGd2CnzI<X-bU}*#c0+uC!-9#>~q0y>@gk9HJV$M4^pK}!%bUtc&lbK+6VfjP6Rm%cepGj"
    "j#56ek2E+k4H1OoUJKp{rzoh1u?+;#D0J4QErsVH68l@wx`|CK{h~QXWAeGJ6tY*sR|+xu?8eYM{W^*D4k)vO&N~&F5l>_6Vp}~C"
    "Y6P><`IMdS`bwt`CAiv+!8K5Uv}<XLd~ijc?YZK2aT#klQ}+TyO6^Q-6%crA8zWgel_F+n7_;D39*3v}YC(fR7#~W}mev`4R%&ZZ"
    "KO1sX6_WV0zC8zHM^V41gh2!-qS=-}M<-vUP^_4jThrd{cF<opI~?}4-i@ZI()7o%NPP@?X0=@e$Hwk1r(?4u!q`&lqA}Vt0;NJX"
    "UdTTRfr-I(?Ka_`=Ysj#8!0}A1HU7+QWta}Y0nD^l1+I-I7YufZ@Q8&>$yD;>S4x9v3Be8mbmkC!B1DvVG^Fxv5}^Sv-UR74c4Yi"
    "+9d*VLiJ0Ltmw<?Q_%~%1!YX?BI*gJ;l88J(PKq>1z^C}<!>}I9u}Mr&lFb`KUU;30&GG7jzJm)9vlP064<GFV1-gKM33EwFY~$L"
    "cl#`grw~j%L+Gq+pPNv|(5vFI2?rsIXtX#^YtcEOyC{rY=Em(pPphM=7$=DGTJUR+Dkv_K^eHuY9B?LHji}qj#3hqJ8p;i=2P~Yz"
    "k;^D3C1KyKKqao<eW=g%t_BjF4`-}j@mtsvMW}*JXf0I?Tc3&ckGe%GCLRgmA7pt)020F#E!Xc@t%TQ#*PR=Gl_O-kx2Kf_*Jqk#"
    "5rG~A9#Q&k^x0MrkJS4}gBrjPIM<q4Um-Py>YfW;Z8i*d^iH>s=gL{oL0AfX$)H`LShLYo-8c#BzbVIaPi810Pj`mj_FVjBy%xMP"
    "G%z4CqM{WB+d{q$d4?XFRoV|?YiOthD>$31D9q9tXo@gWH))?jx#-zPFYt6_TV0B$y0B36Fn^=y46En?Iu+Y366ov@rVwyG3f4%T"
    "DN8`~&BDr(zJ~N%G5>s@j@-1igmRJ40~atEdBv<WAN$1D!}`$N84?iISb`qHRkJEvm6^?Q;{C7nJ{(o?-N4IuOktI>*&uP!LOPV5"
    "ddi~Zma8@h;dDB7vo}#YokLH*qAwH%wtcO4U5U=OA)4Zl5L@x1F&C(>G&|-4sT%FU*oC`kJg2!%;WmqgaJz{bD0aFbyjLv024xS0"
    ">WYzQX$i~3wDazHLZyblTf>}-mN7x_Y<uR*c2PI#AZWAN{OtL**GEZ_L)o3nQgKn`w%v>?T<qtCL7?rFD(F3pNU#f|aY^j9NM!CV"
    "IgY1NOPVD;SG+=?IKUW&Sa<K@_rBqxBsiR4D#DZvuN{)1h<)b@!5{{#U1jx4b$+sToqN?4xs(0wpJ_#)s;CrB7g1K6Ep(P`3^S<c"
    "=}#A+(is($>jM`7Gqg9AK^5g46Rn}F=X$@9QfCm<Yt&(=d5bKKBcmXo#`Hm<)mzI7L{ZWgZ)67)p$l$u=(rK%cxH=hRmE^gLLj6L"
    "rE|J;g)cCo%5>;?r&A!fJ4L3%B$ciikMG83hd#VMIAl@ZwY}1Xz4zUYgT{<dU{rhq+?AAKOv>sbDtZl?m`a9IggCU1rvQGV^GqRX"
    "4qv%iZLjNKWwzUQBIJBB@D_PVT7n@#l=__$aKgGbi(55%u~rh#?)u3TVl&h8D9S3~kLKNTmwx?Up_Z8Gj_Mf?A<7w<vgM?(?PIij"
    "O_)c@1TpO~qY$L^D+Ldfv_zf73tsujej+3pH#080aR3y|pGkNSPbkFR=sFDp+aLx9b!Zam=fQ@~ohorRBE2`~v*u>6<Q1Je7O<}m"
    "p?#0SdfXimCscTyHjk20)@Rdpr3;K2PSaswC`xc~tihP+^D0rN;SM4TnLtX#a~s;0I>gK9j4P;bVF(lRrY^4Y640O;41MUx9`DJ;"
    "__Gzvi!UrJ%JpSnF1Ze4J2x0QBpcaf+Vq+56p7>B#wxDjaipZB(;~dw6ZeRIDYPMjXx1~B=Bw}&{Mj^k!zDeTLS(Ya_xcsSh?`D<"
    "Vtlc4QcTTADLLN+$%U$lU39@EY9Qma;4Rb^#D|U=lMbQ?bn5W_ngZi<wJA5!Gc`u<g8eD&l(&9;X)|NogCBYyTK|=w?7d(wXbLmW"
    "qJ+qpb)$e6L=mTsws2}(zA)|`JA;Q_6g?OrZUvW}m7kK*iB4vFokREQ78c(#$0bGNbOeSWwdrWb>c^^dQATMgGXa^J*Vh|EVP1Sy"
    "ocRb9Spm2CT=4c|q4YuzpAKFT;1C!~sNgE~`5-t(zm7huV7CGerj6nFR#I8esW|*Vj374mTrhlB`WUsJq1ouC9)R+?u&9HCueteH"
    "s99!wS1Da6M+(FC<!iJPB4^ChZ+brARSKs|9`Pa}8p?I_&ePd(lw#VB^zx-P_8v<A3Yip%6MYgx-_l|z$E=Wf6*19M`xT;V_Q&E&"
    "pG1GOC}9yLrOT2E{>E&VeqK~U(^O2@q7{>NJDYN$&mZBj*DQoqthY`pP;z1$CMV@I$NPqcPeW#<VW*TC$P@Lqdeur9opGs*zCE@6"
    "FhpQqA`9ff59}8t%MlxIOf}koTz>M~3z-TkDNt@Yu4p(Fb`<H45yOKz^R$#Ans7}V>-iDmrQJy9FbbilMrXiq0ZAkVNd4*x1fFh_"
    "qdY|mrcR}PFU^_^Ly#dA;b$nX=zW#n{OLxbF)}OjixJmA-O>m*xvJom>C0BqQ-%;b17pDf)}KLX7^MoC3n%m!ow-tMs3D#E=fDgU"
    "{pyE{27GyrYc8UpXgDjsM-d)@CvvtB8*{bL;X1hnc)rpvm?|T95sWh&*hEp33|&>08c9J#>1Z*8L?;EM8}7%n2wkB%$@;p8&Tv??"
    "abGF;&Rs=)mU6=P>daLs)Clo1^X&Ei3O>ji1cmsijWX%w=>R)EGH68kODSZh*X1m-QeT5|Ey<v-g0Rp8RE3H>?=b`vN>x6b%h5e6"
    "l-1n~is_El_Abh7=!x@5(8TBGig*68IFDAGfJLIYS%+@yuuq!SXH=kMQdeYfV}+RulMw=!-Eui5I;C`2p;*ra?-*2~3xh7VUAErk"
    ")c*5T|3F*4DT>lc5l)ahx1Nq`1Um9#AU`G29C1Xa8SuH{XM^O6epIw8bS8DkC%BCZyVZmRC>l6z89Lqd69jiG*YpOdr-raZFZ)tp"
    "!c+XY;4KVS`W+Q)L?e{v+aluB&=kj+Qxjn{h7j9?2r;x`f(}6>#!M<*cLAxm@!Y5Go_(G1(~Q}fIT+6PC0sPxUah3oQTgdpaVT~p"
    "UQbyyriu28)%n*e8OZMX*Zl>eAs-m#KA3!w#WpdNccMm&TWEVIxW$C7iT*l4ZF;5jML63PO|dUiC>7!}FYPFMB@h#hid0UDezV?C"
    "EBa#2=PuZfCA5-mMTT1msVzic;WD)h)cYGgD4FTa`Why;%IkMn&oF?4%7XHe^?ICPXf#Kh6XCSL+27dBmpq69;!y)L+8{ux!z+|p"
    "Mw`z^{E{%A+%t(>i(oyPU%?1iqao3l!QU9q>P<2|tFJYDZhEo8jf;D}Bgs;7^3^>TOm|FID29ZgO)0V>2ru%z71M>e>++*j{X_!3"
    "UJH{vM{mgL1|~7;*8W07T<GVu;74e(Lt87**sep(l!La!5H2kGgR5Sm5n)NiFchsE3H?wmM15Zzd^5RE2U~j<qJ93geFy!)`B<)S"
    "<r6Vb5eTJ?rmQ2$bH;%hQtlN2uG4zA6?Ev@ie0f+XeT}2=~n<tw62~Zp()C)74!$D$rq&yE9#r&woiLo5b5YmxXz;7sAt=PlgcDw"
    "=Q=0PlmE?MoYopd4`wt`@snxCsgL3t{s~U!U<<yX1@HDoUr#Y(3zSBW{D5+eQSwKj<hkI-Xe`bw$Kpe@WJUx(l!X|?6SdOXnS%2`"
    "`^zZeacQ`1$F{F(gC1jUD9G#j8Fh^B9+9pRZZ8~n&Q73znL#(?wiA!F;t&czY!)NPV-JU)2R3qNKvK~iXZ7cLH|LXd;rU8#O{cBX"
    "tRE<X3ymsz8L{Av8lo1|Cp4JspdGHQiV*-kMQ#aPJr)eN!sn=!QutN|F-p31#m<ntV6k9%;1;xHyTxi4Sp`xF;z98#$}84eTJc7<"
    "&jri%aa0DNH&W4KN4$9_1Fl@awn@Qjh;+KuX}zA@@{vRot$Wwk<{xxy1ZJNLx?557CNpX+l8<a6dP%;PIk=0`=b4Ia+=x!cy|0j#"
    "NUJ^wDidhBtcU{y%4DwvzjWbB5a2(a%D!>GCul3$!bHLug53CbqEI}0F?RT96w(x{6<>66S~h}glzCqeBYf$C!dX5V3wa+0TzWtB"
    "{+bq2jw(76CJ!;0Tud2@*@VHBvsF~0tdTk%6(NV`g7&|-cj88d2!1@i^1dsyY=>K&XN#itEDUULZYm2YFVm`s`4EjUNGnl)U(W@<"
    "8YT|C;6%F%0h^bEUQ|KaA=1_3{mevlvFpv*bZITc+teagh;?hT(UbOC@C)7%Tn*2!8|4wD2fQ4kHO!|T{rk8qn3wSn8eno+j1|!l"
    "<9j$4P|>*Np9_A+f|e2VFbAXfO<?KFu@;9Xn5Peskz@RC?9f-9LQPFaMi0G(K@5)ZjW*ENdgXWjRF0Y`%uB(|5Iy_0j=?45hwHGI"
    "&kKl(21c5dmH^G}qD>8v8JCzW?e7Kc4K$2QqDUL^bPNNyh(gX0IMh=rW)>31hMx&|BxDhEau%ALoyJMV(4Kb};VO?51VoRdgQ5x3"
    "V%zE&lMr}ldL$*HIQ1`;l9VEtj&D&>HWslh9s3nmJ9Bo=^}gI>3fFZ=aUFp|ixld)E)k{HE5?7Tpbn>FKD!`}vOjeUto{6So)#RV"
    "j9;D$eiS(7zcH^er_y?++NJYMO<QqGu|OY7xX@s_>G?+m2agDG5}2EgHsYvw9$pYC8uyb)uBW``nL$E8H-eP9@R2b8_NNv$c2^GT"
    "Rq%fehy54^WG!?h636w%^S&!yamp+L94PE-o8!HP{DvM)2({TLfRD7Q$ywV-*`474?HGMS%bC%JK>-x!^Sz%dejhOiCNb_o&lMvF"
    "E;OvzrVfc;jFhLP$6g)7dExNk6uy^;w?1CPdNOWA0{iowUOhF3>JJUk!q7&zU0Y}x7nkB{Tf+Pxvk{J=sq#M8Kw>vSQhGd{coD_K"
    "e922g6}OIWo1&RP8Dmm$abpPCprRd-0}*}_6YEOp;sB|MT<=+ZV1iaDpB@RXRKbY(d%=1oP16x=Rp@elg{mX3yh|&R-r>~Y$b-Sk"
    "f<|p3?K>i^A|BHj2^n6o#9>~T?fL>MmzM}gRHhdJ?C4=&a7UjkBgR6FqKLzV(1iIXL-NdC>u)pNsEUeai*C4A?2i6A+MtdMO4QzG"
    "T01s|aOPI(S9&P)yNQ{NOf8n#t2Hwo+8mTVuyL+Bvx)<+1;Z8KMEdT88bNR-*Y#G>FJH7-GPx4OjhyiDsFPk7e1<0oFQKWV4_vjA"
    "%d3^v-igxa1xzt;EwlnQX7%*D>*(UxiYjP;Dkp8K{An?gCGczMTjDlM7+owtgsQwcqqJbYex%WzE_x*!W^z22B8zf<_QLQwYcZla"
    "pr6Cht0*|57;)Mqb^P_t>o7aN8nA7=9<ZnmuxSibPbsM6fcS29V~U}Aw`xv$l%<0g0wG9w(+H>u*Xbu1K?y^<dOlz2z3&oNIK%R@"
    "Wo_>v<T)2oJB%kQ%9XY@GAp%IieygI%4jQ3{rkx!>Yk@9D?WGWr+1*!SB?w_)^Ui`rFcf}gO0oHdH||?1b9VU?N=;(6g0*bHx$;$"
    "f`}qcv}ipS{N}t2#Ws2e1g5xvDxEd#DZP4q$%4R2#qE>Ej59=IxQpI@N+BppYKz6^H~h&%XGKp#hl_Uo;-4%!2Rie#1Pi7$MYTam"
    "94*PsctWQeMK6;l)(I87bk8fq`1RCi0mW;Ii5PXeX2nShdW^FQ&WM}k>?jQ><ZxB#Fo&<nuZqQ%j*@k!>Eu~vkLAv;V<)C>XwI(h"
    "N?ck|h*(If#md_o$#e##^d)l@%|?va6X~ib){5Mn)t7`Stwo=emXGHOvBAM;SitADWqc1OSV^ZGY%Ovo*xhyL^c}^ld`${B8Lg@w"
    "(IS2>_(d8n*~y2tD7T6#DzOXx7RK`jl6O9~gmtcd(c(KAVjIQ8=71&iI*OmZG$as<`fl_pvIRsM3?{@sYf?I*S(uivz3D*T1C_dr"
    "!agxw;9nR;I8p#ASJ83>!0}x0qxcq?Phi`LD*AMROKyEMI#!P!q|!vBg*I+?T23ovf1f%%eW>D2;?ir=q+iCT{{t@w>;whtaiB-|"
    "M_67bA3f2<Eeyy-FSUAwGCz<v*4qv63=Am6-d+oS`ajw*rX+%Q&o(zD2dI)ZiVmPCxG_xaqk6Oy&(K#{+&vrY*tC5kZT9#kzQk(8"
    "=kAb~Jw8YwFWZbTby7G}f<h48Pjbl$dW}OF-kKp?(YTWm+}E?Wl$S!R-f?-I`}E~s1@7$VhZN?_NW?iS=wgM?I3l=x1+@H|nO=Fn"
    "i;aua`F0fSny1~&TQA7ao(q0jHTv-k#!c2LFD0Z4J3-ZR>&$oBU^FqBV|*!?it>tH@a$B9bzbSa_Vw9@mA=m85a@8!6QVksv2-gz"
    "BztrJk$s?DuIij)s<2iSHe8r5FPu;+Wy@F?#<unOgx3`iJr#;3G$2L3lYX<Zj-J!hb8|LZ2w0}LpvP-X_)2Sun{3n%2K|FoyVpDy"
    "tXI6vyK@uN3$F3Xud&%eeW@oQW5u@FOsBk{7dMtGDG%d^%aB9nOa$Kc$o6^gzkbMsNe02YCy#2fP6<-)s0X9gpv3E`#G(NfTF6#d"
    "+cucK7n0H2RH)Y}(${<~Sbi6XhaZk0+G5eU=iXZnEpmf=>SngFuf|QYa(}jaY^PtM$hQTS3v%?ur02Q6N|+`Hg`*i|CPPPtX77D8"
    "xuWtSmU~DD51e8Ky^^5|gbNCbe_*|vgiwqK=_@;>`b~IcI^xeIup%lBKy!Lg7@fG*c4Jy4BHikRJaH=MS%d>;KC>-$L`g7tFrxWf"
    "@C&7pzGsXjaRE~z6>&#$J1vCuk5TY83OL#++b4yif@O24-b9qI6)u#t+k9fE{84UY7JJU2a|+yTi0jdU6wYdfQJT^asuXHLb01*8"
    "4USuSGGz^&j)6~cXwL;d%_v0a-62m{rD^XkS`_oBq5$@|PGNK#jkc3sxF_Fg?@owLcl1q)7wtWsPk3i7^|mV1LQ&r;F2||U#ceZk"
    "_N|Lo2(E~+G?7zbNCj>3?N;!t=mxZ`m=BbEk*EAwv7^}dAM2T2B5y~hU?!wCw>UV1F+%<h0JYNSL0iZx=*)oDkP+4LjLisCJ{SCH"
    "IHhCOX~^TY4zI*+1aFg&)G$U59^$55s)H7@j$_6nR-bzgyAadTtJMpBE_m<pT?TN_uU~>f*UEh|Bd@58iO?D}%N0vQsep#lc6HcM"
    "{Yhr_LYIoZ<ap^~DS}>m`+~HzIKC-NY9~QqOf18k1~N9BQ@|cdpcrZ_%B3`7O)k8PXwMwA1&UM81wZF(F?jqgkAqqiIZ1kCWr0&*"
    "8yu8s4(#4rP)Yv*FLjSOipXRW2@sr&VLl)5R=go)T&)$I!yzn|x)*!VpX>ti6pAFl2?)E?a}hV++@vLk$1m5Uj>w+0o1VM$C!cEE"
    "vC%g88QLu>FH9yFra#Xs=WU&Kg}9@YPD^nf$1X)+J?bEu4)3-!q`nsXDIBGErb@VbHBP{to868`j8`6-eL7rxT&Ouv4HAc2M<J68"
    "CH${&uiR;#4K?}e;Gw!;F`m}Oh&0bpG}m(~`a~nVA7^u`(IZH=u9(EcsIiWvZzu#3^%ER_sOKxaCGVn~2E@=|A5AHCVzMgqS*bCO"
    "&<N_}M5U=|l(dAi?GE&w7MciGq>ek=^Ob&VP4CGVJkE5$3pzZ)g)5thN;xBAN=ssJbYSQV4uy!Fmm*-Hj3F={8PcytKh$Hv@?{HG"
    "tvyoI5%27+IHO#My%UClN+UErECIP;g&IwvoLBM$CJsGhXi=O%xZI8Bg8A0G|0H*rF7U#j2#g}cu{J?F3e<OOZ5JJEQ!kX0i6RKE"
    "qcYm=BRKVyp30RmCecy7Oy`f>E#V4)3O(|u6>SU}(G{~5r=8J@M37h8$54e73@}xjpu5+>FJEK}Ar;H*q0tYF^%2UuB7q}u<dw>k"
    "Z>I$%b8jgwz`UoZTaSVSW*1PY<<9uI;0J_b{?jk#_G=?eqqxJq(~}lK6<mh9sM}IBMQC*c3U|a-RUZK71W`p%#Qa?Fmb@{YqN=}F"
    ">3b@<({e;*77<Uh6WO^@0mTSA_HslA0|q!)!%eYRC7JS4$I&bLLK`{&>{11g$~At-8%Y7|BXxLC^c+!Xm-{?B(7EmkO^jh>>}jgL"
    "wi9W7X&TaTaU}*7naEE3qz$)7o%WS*M&Y8Q(t(wfft0zB+;mH)FI03wM1;2(CDdQX^tE96%_HQ1=T!D-BT<TbR;0e%D5HfVt*^*!"
    "5xUn~bb151YhQ^u?nTXwyRrHlxhS`oSaCSfE8Pa=t5P%4x56xDJFZ#{^IS^|M-f~>NWdv4nmzKI*lG$8=;^h=pPS}$1m(g!k)F^*"
    "Q_`P-_Wb1fN&hYjE$Lh=blgsF?!vFhM|vd(I3QVC&Qf`v!!MRnbk!$`DBvcGz7}yt=MgP`yb6{m6fJ|=`c51|V5lS=d*6lI#4>$7"
    "f@kwNH&Z_ph5DX7);c(yF`P@9N6%%sE##QAM_Z~L7b<kdaG-Wh30cghiQ^Oo?iKmMbHR&g=;=gsgo=GA8>ilox``;3A)Pw0Fi`H&"
    "F=3QvDf;rxl~TfyeHAUG4EI{_dRij@Vz`;{=!$BOoKAK96b7+6uJl1qbAk_*nQad!nwqBA;}3I9h{R_1TrmHPXFXD5`buHpkKlw$"
    "kHppxSkW?RD!zHGm?t8ldIzIWzv4Wh<BV5=GRAYkpJ8)7&b`<jX{+mJkE<e7Dl+O&15@tlcyH*$RmC9C6#+@0Zql}aEioM%uLZB~"
    "43=8lA9_mV2sQ?zm32JNy$NEJk?PQ9av?O2qA|nOu`&nUP4fkPiTr%Rbd5(VBfw<fX|Q2aM%RaI9YL6)ho1(0iJTi<CLN?5DW)(|"
    "&LD^~2AOic@>9LmXvasD*2LTuySwoB#G1q*J1bJ5Brix6TJa0lJ8Z6o$V;3;kBv@xlgBZge=Yc3bECEGSr91??nEYS)Ei#CkSh$c"
    "_GAAoRM>Rj*{o#<I4Yxzsq@+t6u3M3_k!GuesXalGx*MMA6oS=ND07lF%W|Y8zpx-nw02$KcQR*U;o&cW)p!EZ=C)aY2cmTg=I>I"
    "bN85Z3`87;P;h1#$u{QYYgtCYQ|yrYSY(%UXbMKGXn+r_mWx7Ld@X3NaVOGO+B6*I<E1yuG6EY)3<iLa(Qg)T)Mr)f$wh>X)J$87"
    "XHH%yHHmva7yLN4S8%orm004qr>onc4CQj7>xqkmtd}kP;Wou_Iy7jw`<pDy`kx4hfTMja=&!bDVweq3ql}*UKH{jWwmy!SFA6J|"
    "6)px19aE6QDz1gF5OmcKj#yZ&v%MC)BZ!Lj8L0~7eajkJL!1+bA`HVE-hGNYLL$kW3t7u}^&Tin3TPxjFY(gx65&GV>$}ZM=@f=^"
    "q7AC{C}80MwNNuh&Z&Ftow>Q~)rqC$P0y=!#W*k<R2o)B^v_RtQ5)0z7-|VYh=!NVq$Yk6R(ryessxaHIJQYz&dq~B25riM-BkV1"
    "I)4ebIrEL@;bse2YY|frfmFxrCaoX1O$>p;F@!xBq}~%kOk-#$Uh6brVKy4RpZ>YtcL<eB@ki;3X$^Te(XUA{J&df#$|z;&^bRre"
    "g{%mJM>f-^2-#+IXNs^tqs$javEH&rUNF%1V9~%7P!sT07-1(UD?q#jwS&T!)#oe$>0u@=MV%n$xSG9mD%_pNM^Lz;ho!xzu!gA`"
    "1KZ4y3d@6#m6;rI)On=a%);^o86QDq(*}MGQ=ju0*S?-M1<YJw_+5xLRs=mLO+`p1LibfU{!ES|G^8!7RMn6@=y>+6Q*RT!D*pL|"
    "A4tqmz=)3Ps(3FN(THvZAGhoY%QWW+$+QB?!J<KULnl<nvEwCz<8EKd*RPNGn~xWu`IEs^;!|$g3q>tpkq2k!qmmYe5QYl_z-+Uf"
    "e`a4xuXCA><k`y4&~3k5q0k-Au$)38hP4P7Mx97nJ9R)r;LH~JQ)Wh8E?OOyh3bb08mbvZ*;GeV`SG>j2ZIF;1cMLmHm7>0UH@k^"
    "d-OIkfha)1`-*hy=Qvl5`gBWJG8Po21W#)VuLZx$JSAG{t;eZboK~A&r(eO6nnG)a8jv1BX?SAW5QQm(`Ul2%WhMC9q?_>B>!}4l"
    "^>Td_cVyc(m3o?;6v1iZG{;)X&W%i5F(Du_?y;IiR!IM1*goxG1gXwvuCV?NAvt=Ic*KyYoux^?7w;7H?%Fp4J6Vh=gIa0LR47a%"
    "D_}Q^RYT(!hG+h@cc6qWJnj!E3+e-H9cTUiqnjj7c;p5eBc1_;#fdI`(l$kjY+^V`5Z=tyJ{SC5;|qz!bAK3cZgkivb8p7=ebph^"
    "NDhF-<D8SC=|ullRJ(G+z`!m=+vhutKN}yNeo$hmanmFt2rrN;I=W6b6z;BqggYoxYDMi}z2BM{CB>)h&r5s7=M(<S&r3QzSbZd&"
    "C%oe-O4mmT`>5M1q$uPHgeqYu=rIE+sPCczjKV5z19{Jvyonb4f?Eh+3_A6d55%`qVR~gKaB&g>T@RKLrAR*#oAgw~IlQ87+X@ZZ"
    "xRU{_{ao<M3}uhruGoW;oXN_v-0(8?7`=|XXg&?Bc>x-g80e_MI3dyhf?9#7V>a*5b~CX4)U#!fV_LcCJdCdG+G9Z4uq&9&bLAuq"
    "IuVX;Zs~<9-(y#xxWpasa?dUEnY$;XXt@YtK(vIt=tP=pOznGKY-UzfAe*iTK{YhGcFCZ9k%=@gYm30iS9-dZ%cxL`Ezngi@P*M_"
    "+?!9c2R`6pHTpmU2f?G_Aj})1c9s}3Z*}MhhC0%!uX!1*c)QBE86#T=8E^e59@gGkb`ei!Mxts*^~=xLh0n|phtBAl&m!t7)P@Dt"
    "OEVeA^RMZe(c?29FT9{rjbw}tc}HphOdqVLI1=@O#WJ>zY(#KSdpJ>WFX%g=0bHId=DTaML2RUU&vdU|q^Ba}5o`<e=LAOat50?e"
    "L?gG4!K1$?QW?vx*cDch+1G;p$GGjWEzl-tC0kMWEcB@(oq4A{?1{>@V?ZV=_$lxRS1gQu7$r4B`_&4bzxJuS62@FN@q}5S5$j|I"
    "MLE+xAsy;428uMcLi{^y5#5@GXdTw?Ram$yiTLLerdu`97og7@2aQG1?_5P-5e$9NH_YkSayG25l#3!w(==(!Y>`1!5E{rvz83s|"
    "VyXfeTpyzDzFRmKPh|*B+3FC@u$0*F6zQ0f2Wr9ya*wHBfXRH<27|Z07A#lGurTYIxzFQ-t&sLUZLvCDdb}+>toVX~Pi5R9yLH6G"
    "OVK*93fQ2Mx64ZclX<bP8Iq}RW^@dr0C5o1BF@!F?iwt}>gK|p0c^T#3ObRrohwX*gwipJk*^CxTBpC1U&^s6iY@3j3qjyd>x$@#"
    "z!S+F-@YhL2E5Tv;1liVTFEmXJS@jvZ~yrbKk*NIgJR-t#ulzAbsk)a;@|79!I~BbJq~1|8Y&9PG!`FCB)=x1NkK0v8^7YIzrFJj"
    "xo!l0@y}CaONm=VMYBMA7IG(wt_^wE5~3N*S_#{1^m>FYT+g=C_UT>=ez#BVOjy!}C04D2wW^|<V4`4y$E2l-I*^CN1wCpJG?y!Z"
    "pbYPAMeL?_aY|QT3*HK?E2-9ef=$!*XYy8|0BK}FoH7xgtQ~vS;nK1~gu-;p)X;kqn2jLd>r2Pyis@dkDfuy<$8B>t!u?Ibk66xa"
    "0brY!YzB?bD5ubdPQ%81h<m*)aw2|{UY8}pLSGXt^?cf*^5(X+Q8iKo7^rRu;UJ%Q8s~ZkF=4S8u%ZwvRpi?RmH6&fc$p>8Z+(^U"
    "v>T3&X~YRvuNc3#gpY^Ybd+aIJnF{AlhG?oJF5^Z&X->AR|ry&{MEIq1-~q8(&-iyoMrY2<$|G@y0LFo&>f6G(@>{f+eDT0cN>#p"
    "6h5&t;^JA+@HP45t`jOn>_WwK;)%&UmZ*wZW3`Yx>hha3yL3<jeHe<KLIzp@sSK^Dzt=CsZ{)RLyc%jboIdDP>3!5rb&UHYBKe-+"
    "d4Vn+f=#Dxt3IP3wee_Gs#2I3X_V8h@mw%o231X^5o>4_?g^RhTbN~<>gKMQGKoG{>x>`-v?3rLLL;G~&b81|#>nKgV!Hf7j4NSG"
    "DjFfEBEq?S5eX|8Y8JXXmL>b62WfQXM-X7d;bh^?7V(xHXlFgo#Vvp-^6La}cQL*GH0p<XwAKfrRHAT<yd>ccIxbP-M9rn>_-G$j"
    "VjSJ)LO8Fq?{B*xT$iivl#M$@^_nB;IJzlJjJ&u0?40KMYx1rQdvM|LfxysIAfj7aaObgLuvab$Giuy?GOfmg=^tes2ErkNG$dP;"
    "cBfTVJhm+ex%II`Vw9ei7m%8QZtL}kcQhA0Y3EOH>(rtGE`Ycebb+&lGN1DBk#t+gNHI-|hC`yrh*dJTYg?H3`G{W|Po_{FPT`v="
    "79|~}aL<*})?WllMRKQ}6WROFv-jsRd_l8<!1J6dgsIH=e8Ml*)N|PhODvfIS^WTX6s&=d4|6htiiwix;Oki%W?4|}*r#dA`haKU"
    "ve!HrrobO|Q~2g23T-P#Day{-j;Ijh6wJmsL8vU8Bf!%`r^c@{$yIdTN@zY)pjTpfE%=>8>LEDY;An1=ICvBo+f|DSSSzE8eU>e7"
    "FZ!m3W?H<6PEMK&Vy|LXR+!bi+-$X8ypF{B!)V`I_&d?!lFr38VZ21HDt9os4iks6i1L)cy`z(m*-FOdn-cLH?z!Msr&VBd_OFdr"
    "f!vOw0OuI$M2;RxKa_brT)t}GD$ysUPKJVq*pxq)zyb>6{YJ|z)?|yUXguwsq7kIHTG8~?b`|Z-2!Mcgw3(PAaaggou4f!`WMN~_"
    "$dh@8kLlBCb}lcHHaEgaevv}9c4fi9ma(|q<DUAC%%A9JdZ7SI_x85vr;6zz-tk;8-V6*)R7Mo|jRxJxJjIZd`bGb!Kl$ys@|^^9"
    "M9uZw+}TVzzAGJ)f>`5y?(3(C!?0ClpgWE}x&a+7WM5P8a|y^#?O`T6O2E_OX!~WQF>6FRy5hbb8#93XI*0Kq3=3Rqbw*{mp}?81"
    ">$M3n>8I8f7V;M#w+yvu_bji6vp^cuT+jGrJBI8!t<MG19V9K4o994+jm!Ym)P?+gOf;4#m~d&Rzap4Cl%G1LA8<&|&~;#3Pd~zX"
    "F8DEh{~v=wkqA_rlzW_l4sF~+5Qnapsl7-&e<R{L88fZI+odrXv1?;MSz+n5;MKhrNc)eX@M2(66wV>6qGW__XtO<wR8(O&=*~L!"
    "9p+pu%#3ay72_&+^1@Mfa<%H)F)#-=#Jpr8vW+pJet5;5LGP}K6yLDdEqJgWzk)c#@aU!y_VD{PvW@}yRjR<y0U<m@QNj$k9fp{6"
    "peKlvx13yyLfx?*jUW9uV#UB3QKwzF`1AewMz5Qmb?!{)1&e;#UNLb4vUL1PM083YSh>)P8CwSPQWGjwmNr%&6XslzP`9rMau@%L"
    "G{&ErAw9cXhtkEXkBOZvE^>zE6C9r7rf);>&Yk;cQrQ*`$Bj)7+xv6D_<h2d|9nDqDA;;Q2wyD5aS+VJs<t{jgbFIOBM^YvioR?H"
    "c<7uKw1W7*7X0pIY26n=Wf(~xpW1?lcLq$Ml%*0bw>lV0;Zs0)3BwWJqe6TZr0NWHi(m*UJP-coZl~B*lgI5f^%e2l=txp3WP0(L"
    "mYKCiYHTsgAq-P24e!duE^l<D6#Er{-h?fFb0uDR-qS~mHP`0)N*__^QwMs)0tz-=co1<8q_8qoK3}`QVa|*3T}wdpPkIsWm<MA_"
    "Gz>bdNzm|~yOgb$R?ActQanp^AV-6`Nf&1wI8h2I({v)vAM9x=c;%<`RWcmc@)T>hw&d0tHmFZR=OsfVrw?=~>WHxg{X=E~lhzuK"
    "B6XMvIb+msUkkcx8isHMLkY_1?Tq^gjRMmaRysanbp_VYfs6?ntT#fNWJYG&VyzjtzIkayAtZ8juaWuWx*H1_@hL0{Z0=crjgBCL"
    "#Y8e5H6?EMg?4c5))7RAq4OKI39or)cbz-QLk9jN#K}dSkP3KH;Mf~Ov#F4h(M}by+xlIK<Kk%R46Zq`D<VCC=q32&QoOOIiZlSD"
    "xE?LpBbVH!VrJpmTrYT*inE&lnl^_uJ2SFMBf2eKabf8D3aF3gf_K-TqV{J~mW031HlPi}pl>R8Oyix1fiM6%7Mo50?M{X8HTO1p"
    "J}#szX3@_TzdI;Y5Krwp9wV(#jaFMlAyuInWu+r}qpzeN(jg2<dlse<Om`#JgPi)C&ZO^JuN<>cg>UZDbj0;=J9ygYgaK>7W4Rzs"
    "1yh`c%M~sjsmK?-^ei@vd%^SmB7L`dq6^8=5{a5O?p+S=Z4Ynah{Y%jXQK5>DXHOMhwq|@x}QQhg=%!g(d#AUfbjDjyRZrE2)!K|"
    "0KMXsxO}I_(&-_henB%;KY)OTQ4ozZFF=}35l(QYIo$r9yENVIYr14kvzhmK!<=#$SiZ#&#FseP0<YMU4krkJaB(N-h|^fFFUc6G"
    "KKHBob;lkHs+pAAHXNeU;9NO09iL|r`i=1R)+bw*yP_QwWu{2RX8vi@)@scv`!$T@Qn}N>TKrsJAjDPkFsn_}?@ZIva+eX<w4%pD"
    "v2Q7=3)1DFbXl1-A*j}ctnzEYD|^kbYC2Zk`jHa~3Lew-S*QauXKh7tXnsC2v{;w!fZ<~i^f3dy%B0u*e5LuDSr=_dG@C6aM2gVY"
    "6k%6*Tj4Y%uYew1*T%so#9N%*wXY$^dYx3QEE?m+KG#cE?jAE@9m5s|;bS$^84@i<<TlU&;Wlmi+FF7_PUMpw>FTL3IyaFpw|R{6"
    "T=2SSA$mY5ii?3AyUx^h_}i2wzB#wj?NSzrP;%vo(Dbh@ty)5g+C4k9CJyydhje!+@QmUzBQJD05-#k5H5SXzm2rS3nz>zRdZa9k"
    "3q!P9XdFF9TgA8a(0;vaAY8uYgeMO%g{brH{LF$-DQnkh4ed`n&^$BGjR6pDfww3Jqq{WIEMI~`n-Jx*kLOOz=}?E$Myo8&NHK!~"
    "ip7dtm>Kxpn8dAm#d_TofG1v|4Q)<fuo5r!cR1O<7W}Hvp=1{h<)%J>K>)r`%5$`@DFOxNfT9!h&?|6Vn@5mCK$v+t1h@60w>9na"
    "jmGO^t>1d^1-<|KR1<hOy;eK%C=g#lv|A{L8gx}I^eFQbZ$#9Vw|sB?me+c>4i!CyZ6{SIK<iwK4(c3q%z>F9BPJ+GG4IPYaBawC"
    "C}}E!o}yS>V*33_NAou#ub<ML3N%_99XWKNwnW#zVx+(>z6m}d<>IYkyS1xWedvKg9MBb+1HIDM@UhF~4tWTH9*}6YL$nbsRsjZS"
    "fq_wBLiZM5)Qyl*wiuvcZP^OFy3O#aE%s#lx#9;)x$cEpU<VePRoX%NSab+2rBIfBd<3^Ak}+#R)HW4QPy1lk!5BJBI*9K_ynVu1"
    "448}H5zUTb6x#J`kTZrpnIND56de+ST0b%YA~M+b@?R!~zKO`To=uVYa}5%@jYmq!atyoBgYxVe0>zfAI};V$BRVpPPTK6*xh5ww"
    "1S(^~S0huA&d+oBNw`FWIk-YhI`Sj!9k|WYcRi{mRMqmx=sj>}BN&;Q!nM$1Fm%F*g$#uzuVeCD@RLinrC92hqP5x$dd$siOguKB"
    "_?icj<3P34D7+P%w8rU}Ml*g;!POgicbAth9SU9?M;&C*EY{njmourB5dND@xFaYv)+Dqj3fV9(!qcNGm1SB)@U`4eP<p2x1u=<V"
    "R26LON)nl7;o|bZP98yj5f^6g6?b$1j((Xa2KFK;HM{jsI-GcRzgD#O%CgWJ?69!s&d1|9G{u<o(K2Ec=q?!1CLuP3Z+Av>J&y2="
    "Id2pkopIfa{9N(ZK(E6{*_%1rq_g3TrGalNuiN2Dp_L^d6j8Qil(pG}&`A}U(~p63iayJJHu4nbFHS7xG)U}e9gt1$&%l!wnOBqs"
    "jdboVZC-Tj41Gn1IuTl?dL=3GN7Uzf*TG)!1Z5*7qT@-Tj}uXXFEud^8!fH7sKyMG>e|$1wJ6c;qo^u)MW58O`>T9?t-?kjlb{&+"
    "Vvna8v+}9F8k&EY*mEzkjADXO_E5Yv)Kq&zZ)FN4Do|i=`C9NMU<{_HeQA>qOefI)5MR?jXcK0^GGTfj`(hFhXx_w>m?&lb1vR-e"
    "lvdia1byc$aWOqNC8bs!KcQTd>qTgyeocvcP4qazf#3R%i`mDhvl!5lkH}w+?iGQaTPFT$3S65YgcrDoDA?L59V%K+o<Uqgv2CuH"
    "XoJ(|Zh6}>lIRk%^7PtyK>Cy8-^or_VS+U67fj_U#f;RBqE958E2v^IADALYb}A?0fK_J~DiPnOJmDwl^0{F6nc89Ah2HBi&T4}4"
    "Og7Ja)NX}~J2Ma&D_`6b1c=K<0lOZF?5fYIc>c;3<x?@mPA@wMY2#UkMM*R7DRP}~$5n<fX40jjxZMxm5Pd1ucQo%sHfC3vG3GU4"
    "?aD79qCs8I7F1MYGM_;sui#DL2j3D@U6fI}<<7m&iWt1n`_H)v!65>OiPCGq`rVXx<M9S!HprmvG3@fIE5Pc24<t61qL0#~IX4UX"
    "y!>E>^6=b?%F&@*W7g+_mjTCcW=jUwA5(EaIjCS8>kRGy2|IG<#=r-f;qN3CqFp4q$dJ-RLaHh2zcx+%T#AbFt)Yj<TYl7t@uCl0"
    "-YX2C?SSW0pkKKvu%qr28d9lT7$uyVXc!|xy%u!0JA%?L=Yq}{eX=c@$%wo~&&O&<3b39UUKK+W<6)Lt>F7j0G<q^Hh!LLWiXSCK"
    "01llv^unlLX9NZG2h-6lLS_dVTfvCfiDoriHD}+B+e4`35-QC{=kT@Qcax92FM?Z&HCZTI96|xd51q@vR77`W15}ADSV3}2oGo)1"
    "t0_mLoj82guQ&O*;^$b9l+xp&qzr7*fv7`X4KfW&sK!-2yQQ#CClP(ZI<iZL9<f6OceLJ8pU-m<?{!zv7GeMy&o5^S;_nj^V$bXo"
    "9-R|yuf=rI*<gy?d`TU{rMQdK_%f8QUn_ns?Q~)}=7FPt1+bCotwsC7QI)n$`4$&aVP?Fwm<LhL!o8LeCL|+Gb6hOE7R=Yrk~=NN"
    "bv%v;O?ys{)@Gv<o9ib9a*~(MBKoP+5NLZ<f^3~J*FZC7h%a+1ERU|q9Q7pf#ZD2i4};w?rl3eZH)n;|M17MY+S*BvyJ*YSk~6Q4"
    "dm7G>YmDcD;Vy{O1*Mu}W+RFsL|Fd(ItBg((r81>%*ZA@Nil>GC`Cw3$EG0{vN2(MF8C$=N}fPf1uHV4d2w!v6hoUL9#H`j-dv0!"
    "Ih68rYJ?@3b}n;cD(h1GzvvgNKhp^`lF#t!7(Q_BW*iI+1Ukqw<V^2)>cneXDO!8V7#o{`7`Xvcq_HWz!}rH$&do&vxotF`_0En}"
    "?m)`3p!=+)chCl2+O<PCF3;FRGD0d0D-ZbT(kb!p6>p0<4T^x%*e05L3zwIX1$>!>HCyaV?>!2-&=e6SE(t=F!?!N%5gJ#GRbb!|"
    "Yy58n*&@Z1j{l2DzmAEYV$uqy)1o-$4}Cy6dYDwrKxalrWG_bM1mTso3+@K*1^v|$r>KB<d7Wo4CvD7dMddlr^tVi-r4c_eLYP=i"
    "#HG(`&@w+$!;10i|6;+f$6Pe;sBj5S>Cju0y#hKW0=`1=WjEX=uwLt})&VuQ4C{-xHKc*fK-{RlSgF6Z;1RWDIuxsI*em@x2N<FJ"
    "I*-MKl27e!n~WV=iQHK#X&{?}0giT15ZbZ(wcgKI)-k6})~6ef_T%B#tcnA|QyH3jY>L~0-Y^x3m6kx9+fEn(|L(;F=BsFmFxAJP"
    "6iKjw$cmkUNt-%U=UOD=sCg9M6$%uayx|hF2EAk_5=<|B5z7d5Vf^%b#IGH?FfKw0_T&cUWqc!=)K#li(2p+SjSyINTihxOk#McC"
    "l`sXIgCf6Igln%MU2nb=>;u9<G$;aC2W9rbg#u%gVxJ0Sh5^iu$&C^P25<&VSEZ#^hv2ecdQkT2uLVCw6M}d|rawa4k-g7=cxdQ+"
    "2j2p!SQtz!1ddv8I_Uv7I+H9O9DZ$+ure{P_3jiKx|`+5?Rsqc5>->HQ>}bXdKI6tOR&XnCXWZ$idl&2uw%~%t&XJg^J*Tz!~6?0"
    "3d<aecCMaKNHt~%#3RG5O^NQ6z+$xGG`949?%!ur*1(OjZRWy~;?nyGKWKU!b+ALDXg1LG6p`pw1g@t*U7A+eievpYT&U2dpqS>d"
    "CvrjX4rlQ8Y90{l`@pxD%~8R$LNZB-JT(-etezCZHF2}0aGj$Pi2`V$3RlEpu)=Bj>4uWxnzq;HG5owr#1M5ZaAT>snTi4G)vhGS"
    ">+nS?<|r8#ruOytHN)>+)~-=VQi`MlWoZ|cXDH1-D@2PbIkbCI56Z0wMs2!kgy58dv5sv-JxmkyLC4X@6c-VmfiT*dw0nJdt@sUg"
    ">jxI=MP7l*1jDw+b+B>O3?~q~7U1>5h@x}~rv=@N6&&6sHVr2n;C2Y+`G_B?DY+<X7Y66>6BiSVL#%3WV~Q)U;!CWt2K^wo-x&Nm"
    "x6+4+OD`n5DA=R3Tb~F2lQLCm5M9DEd!QsB%4S3<%A(9}=)I;_SM;Vb{H(7DrV;KQ8`oupqJ)PO9e0lf?WYxoA6*4|cWwurKupNk"
    "9_dG?<I;+%#+Lk%b*d;B8gAT$LqeDx_e*W>DQw=J3*P3o<jxqIr741rIl-_<bEIf4ptUP<S?!R@H9x)PO87o0<-#SMGNB$%nCkt6"
    "_W}wHD0XS0m6Hl@(t*U^gS$!F7+HzPxfef$cX0YCNMnsT98K3E-OOpfzM;L&ixvb*7tdsnEul?|%S8rEVGyWet&K~X^c_d2HkBSG"
    "EmVF~1@FwQSWT~8YTc(pjZ`{4pNoM21(O*wGiQR<McnPwGFGp3YWEsJQH2-1`osoa%N>^{#@JrA3|jDOGR;^S(yd_Yao<kUBgvkW"
    "D}r!SK)t*QD!LVjb)DIgo|!VKLbqn&h0Zj{Yr%U>3W%nz096!7#6-*C?TB4g$A-5or9MS4<wB9dvhiZqiFW|Vpff6}EH*Qr3*Md@"
    "w#w0Lq*Us$T}w!T=nEOMDzJNwS-Z#cW_N?&XGAum3M~qm2BV#8s(mh)u1E%ha(Dv^&yFB`OB+37aY5O3PekDV#A~2u$4pkqN~a-Z"
    "#Xm7OdSU8otlQtwd<ap(8(8dJCtSIKp)KW%K$p%~ru~^|BJJlEw|?y?LuSQ`!Da0fde*miWnlZ2g%$HXp;X9^R8Z)*@<Rk?tcK_S"
    "SgkW~tw&NY&@PFIlV6Feg6I~n^&Osj<{MwF%mbe?hG?JLqQFTBL%~kWccQ^t#Qk*^cM+NtdD07s*gNXM2Oi;A>M_dq{DiliUCg5X"
    "aVM+SgIW{)e+D6;N-4THJe`%21;cmJIj3Odu$JuT0U$&~5C3z$50xUuPNSJ}fGN}}yI8N{IeliBLLe4g9YQ#L3%RwCkHuUPt)!Tt"
    "uRXYVe;$gjis2}{oqX>`5y`80b#Z!65mwPtaHi8PMVF=0Z72eE=rQ2<ENG>d4mOCsJ)iL7`1MSc;Km|^TbQ|^smt$-g`-Z|m^u++"
    "rNiztmw4TZZt4Ivt`T}w?oJA|@nxOqe{<_qECO{wnCGBxTl+Az+=K%34x1KUr=5O>D%#<WJGp)c?s8qRIi`Z(%S+`18hvH2tF73-"
    "Y^OsId&Gu3GvI{a5LyvJ;7Sltk69N@-dyfC0Sv#1xC-wG0jB4I*Q7`&fwGD7Zb6-j*a|M4(d=V-T!lqNIy$CTj3ZOOL{qsaJE7n>"
    "XV8~@nVyS#ex<K~10K0TjMv#N7CJVqU4l_V^wy=Z7eaB75lFg_t#+4DKB=MAqmA1VzEIBvzdB99WgRz`EXv*#HigRYT*90v2yobb"
    "x~(fltR-T=iY-lY=nyTs-E>Izcn#)j!TK``i^$JohZl*{%^-xSpgLULNeOBl7&v{HOX+DMrrepp=znG6Oato69n?Gbvy{!pN=qN-"
    "^yRoCM%A6^h2|ZaS0}P#d%I3+XjdXc^S7HW#M7}4Oth&y7yPR1Ca}?IIl;4jbJ3J6mu6XKb=k%ImRO6yZe(*`n2a07&Z!{h+|Ys}"
    "LzZ6)Ucp8oOyejEb)@8RUc~2B1k}@qZ@F48$TtcSF2)C-=To>4B~F|aXaP~Ydqo;|%lekSF_=Zd!A(Tw3`X&uQ9@yxBBZvJKmQC0"
    "*`{gg^iD)FOA+v1;VDL|1<M`jMj4V#Q}T|ytTLA!sLayuILs<~D4_`DL&qh@(D6~SJS7}OA%$+y-~9^S?(5mr%0yxv{Jj`%ZuqcZ"
    "1*(uGVi6aPo$g4FdY{k-3d%~#!_n)DT8D+b<)-zu;Mb=VP<(}~Z32~$AX*gDWQ_A1@|27L5yB}1m>NWZmk_DG9)c_#-PT^se5NON"
    "eJAv@Cqf%VU{qd_yHEz7qhZ*G>2PFXh6WnMBaBx-Fk(0u?&-gItr@xAe=S&lYViM2XI~ZiE=nZ6iNOmd&rJb4Z)}&V7A(TdiNPVS"
    "56UobU7}k|oJVh-+w@Au7_OYyHj$Yh*qHb9({v$R?R=57p&g6-9_BsTDA)uusa!Ye<)Z(v^c|lswO+T9{3pXDqCybEV=!Pe8I+3H"
    "K?c<m{c{qEqsF%sZiPsj`O~7;nG}L_FTo1$zZ9&OBJiD$D3%|3N?|wCk;lcfYUI)NmW{`kV9@h{ew1TCaIYSP@DnA|QC~`?2<v}C"
    "f#nZmOmyhA|EAysl3O+~pkqh(lM8i0Z4^5a(+RlQ@6fo}6WnS^VIaL1ywgv>aHgG?#p+bV*G2{LMmPP+N|^Lz=7cBUsLB?(?WUh%"
    ";BXnV%Sewx=g;-N_6Wnx3|ENad;17GDCI2W-O<qw#vohV*ALXN8iKISZ|tDkFm=y?qo>|&+|O_Ln`7}h7Sz5Xv<yw(j#ayzh)6{2"
    "F}Xrm`fC!28hU(upfakp=mO?Gsn3@9TrhsdNcl_#EecL~a`U0Dmr57KDf&|bzOJP+wAK-gtiS>R>0vofw5iaEww|xlUg({}8G4+j"
    "cYC)&eYxb03W3r<@)PY>kq%^*6aW?4H&YiLb$0!9XF^Xe<F(*N@dMWbxXN~qB8x(YMNhk#T1~{W8MWquosSZ*Hpqx{M---I67rtt"
    "S5jJe!M2~|NU(ph1LJs`D_P`u22%wbMErz;yNIy>g}7+JccM-7(e-ak5b4J4%g|-;e5L-UQRQ<!(7Ryh!;E%n-I`#m78k4*5AI46"
    "2kAtM0~#X4%9)i}=wM<)=KPg`{g?9b@+e11t>NFy3l%?0w3_4LI+f_rYtbw0m7#%|^jK7>b6+61E}ZI*n9Os<PuV+on#J^pR?*DO"
    "Y?0O05ux!BUn|?`v*{&_iJRA`G=<RBN;Fme_Lv#={(Prj&PRXBaJv?Sqwyrf)}?l)yp<|Xt=8^|kG7(>$ZO|jYtqz!tF9QU8=?r~"
    "NrC<69wH2{_iMSC5v;~27B!lM*!ye(7dqOEk}1h5<XXj3ZDEwD=A^5T!7$vzl6ii_Pd+uEL}`)w)a#0Iw&kWmqiKw!>V$3vMq?Zb"
    "f1PT#5=1qLOD==Vf|Ap5e#_5;copwWWv~Z9rP>5O^&{i06Oe*T+{O~Q^i+EG*6GczS=&@GAOI!Be}iW>p}a)>wc@=%Gz{g_%06(>"
    "q3sGz7EfdQa(G?pf5+itd*nr<_6glgIuuF*Q|aHmL)+I1P4V&A<|=+<T-_qq9{MW1qoWWyvyI$&his3TeDw6|Z$(%A(t#Xx)4O=d"
    "jCp^p9+83LvWs5-P1&-DbN+9}&Lr!a+_<58(O3ia&2rf8e+kCrtK?TB(Uy-bRh53e%p@a#U<4q-*HMdL5Dt|#Bu$;4MzLFmg~lS6"
    "@G+z=j=Yh5BaC`SpNxr=yq6-S{!o!7C)VU6PE1Ob4i#hl=~lo-ORfds!26u*wp<i@icrT>gG<ZY*<5Uqkp4k;E2BR{PgJB(Y^aiM"
    "L7U`I$y<ssjT&jBU!dvSoW|3=(8zubC18kg+xMd5SOgL=mv}jw(J+msGo-CikIy*X>P;YAzfdo_(=DX^yc&8j9l6ohg5M3PcDJZ6"
    "#jl`Tj)ZYMgZaf;J-9wA6G$}8DO^S{9k)WZ1bV>w5O-|LjMDb2juE~KL~YoB#ILqEV&UkRH;DHD)d<MUXz<sG2&U?9a6p<Jvr|9C"
    "&`c|ec|A)}Azjn+n)5M46K>aTQ#u0e7G)sq3dHn>@QZKtI407Oi436DOnO{qlu3$o6FwL8S4>H8ko&wM;t-V)aT)Kl|2Dd=Jw<47"
    "4yD{oom2S;`5cJI6d6T;WE^Rd@m%o}tBA-fXP|x^;Anf~{CdDodigqZ?EBmb-TI0+SXa!)3###WWGrr|uq}RWnk#KYuLCKc`+wId"
    "oDWgHfKJ+GGSFA%0&xsMz*$@^V9SkZHqugnMrT`iZ<?=wX_+{zyi$j47b7!a$EMSPj%YPCFF1bcNSsWJiI~k!TqgRXlT8nW*iU#a"
    "=&wHta#3gsH(}ZuOSN|Uo^oE1$y5Zf6`bK9!oH~sO6mvbMeJ0}V(>#ydF1(A@N2^gNT5P4nj#slvLe}H*tD-rl)4bY-nb^AR#W}-"
    "c{i_=$*|RC9tjn;)!t8-zPl%sB0NF|QTs;mai!&#j^Ea9z<)3cP;vcbnSGwP-d0?oW)%3w$x!D7qwvRq@tRvfNG)blVd`wfjPkRd"
    "WtSn?B<iBqDiGn6>?<--F_b0FUC(fymeoNKMM&CJuNCi*e6T2+{`WsWnIVVDmdIM?=3L;g&!NXs7WDMeY^;Kt)8@gW{J3*lhtu;r"
    "ddF`@-dDWi+^?)aalgSU6t|=P$X^?~h?@?Hb`Dd#3e=7vmd?N@vdzdlwDP<Q`nLc`IQ2b0#p@0CxlkfJLJ&{a$IizJ9mDt0um~=("
    "Gg4BAVqT)kl$hFc&jmjLE`(bVy+@>!7OXXteK+ECbY&=(M~(nJI%94$0u<snD;VH_utIMbyuZ#xyaU||rN>(%m+k225LhWlVKV%u"
    "*W3|u&_4F9=e)UDR0P1h$3<G~xBeoW>KSUT3Ar$W@FnKQ)go^@ATznsnV|HJ%(lh6!<I7joM;M04#z}gRk4RDa7BIpyk3hRFm)*>"
    "BAq^>;)~ui(3d!8bv(M*&7NGB3U>7(lxR^}751gDAgpf?$tmK?VZZ9$@kUc|Qn5sGqB2ks*&|5^2`d5Ws0-38gii$|i1K=3y+}N^"
    "vj}r;!+LX?9qNqF6~B|nb+f0{Oi+xj>MCxiKRIqL>_=r}KyU*&b|JX{YBF%94EV?xp=Rt6%YLP(_z5_1k$XCY18y_oYgN0@>CbK5"
    "mlWb+${5SajBerJ0?;L|yto_vVSDW+a#LRFMmj_HekKcp9=Aa6aLH4~SD=YzP9{wJSaF(}nof*W=<fK`<Ak9UZHX1WcYlNm>T|^_"
    "Nvk}dCuEq6mpa93yNy(fO*AwqVJ6-1cFs#nbp~&Ae3Jf+Uh`rOMW_FThOZB~XKsb8uU&%3kHxw&KyZN<dUZv+5hof8Ugc~C#^-e4"
    "^B7c6_hb~}*)Ev;$AZ0b@DVb0TXc8Lp>y8Hsb+gUY!<OfG`Y2)<e0cri-;LFQMV*c6;Fe?($#at;I7jg6O%~NHu2|(AB6z_QKqhn"
    "+YMQTwHMB$;@H_3d(_+<W?tH4ff~5*>9dHLuGO5&mcHG^XjfMJ#|+JQ#VkV09Mu(sh4mX&$bH~TQI2jS*ZT^LdPVxjpENjq&@GjK"
    "d^SlKEd$!(JyV{!<<QaX)&|i#2%rEu&~)_lJA(cR!-}C#6bkCIoi2UB=8VEn04t*1N7)}s-l2^{0X$LFEOah45slH28y>WJF0l|L"
    "tsWS36F_m{x!@g2U}l_8sE>szI|T}D)2&En7^<jEjLD-+HN}9DjM>UmBP%|Nfz(ahrG@vo;I$_d_CM&fog>KwkpigcloGR|fD7D3"
    "+qAgVw^lD(Q9H(^sOYx%jWWvYG-f|myz&Z4^2NGvR3i7pKWbt~2Ai>s(i%6nI4F&|6bR8a!dgS;rr|)dG1f+?<k^LJZKXwOoJo)x"
    "I~O&(A&}8$)-whq!Gz&(yUM5-mWFHnSku~wN8(~49RVnTk(Yd`B1-!7@B~BS@kLWa24E`-m+lgc@*UP|R`yh;W-?s>5h2%vQdP%>"
    "+M`ZqoPR!{zts#(t^$J>RMm{Db0VsM>7xrrT%}EiJOC<dN`u}60OM9**kqf{Bvh-fSabCi6zS;}0J}voIFaF|4VXk$@L>`16-X`m"
    "xETZ>m|u_nebr{p_=Dn!DjX$}^IGsLDl!h2kE^AEoA?Y3@f?9MQ8H0_)`{=n$V$)T&R!f6AR@{k;GeKB(s@yce=hi42J6q!PCZAT"
    "hPh=LQF?U7Dqx7Bx=6^la-PzMro><Iz*Y8``pjjWBl>*8uankAWT{~o%83Y>avfqs+uDda14w2l6Lm2&#|((r(kcR_Q(@+G^yv3-"
    "dGS(k(t;N(incHrh)$vQNo}??hMMPz-E|<6lhIKVv8b7L)=iXI4u3gCAw=W!i(U*P-|-;9sM9gn9cFc+eFmvi=C#-G+bC6eeOhF3"
    "1oE3Ao88C0sLmoDyUNA~J3dQb`S!IkQ-*zz&hkc%BwB`_F$~f&W-xIq`Z=_en3EkS=}m3sAr`Ft0#g_8(3W^Ec&UC7z&rx6M;^Sl"
    "E&i=wRu&^)A~UDQ)p9jN?~Ou~&>D(^3LQvBDxy>IeBzqxx{P+4p@UC+Pe2JJ=qWDEK$rg1nfDS{q$i5@vY>C8b@sagFaX)viLEZZ"
    "@bkIgJ^6~=d_;Sc=P(fJj8WsQfncQA|2S4UT9s1W5=d3r5bW?M+fdxy)7WPXtbBffIupe+Y>DG(TN4?V?8GgS7q>TJZKx}-tqZfy"
    "D2wYQt{`?iD`H4^4T-YG=Ysc|4Q7D^F40pgUzuTnLMVv(18o*9JZT!yRaBhCYdRhW4Y7<UB&0YXB=TC&-LV9NOD47@g@L@J|C1oU"
    "oVsB`7VA_vVde^}&eI9Gaby=M^~^<~)t$?;S*d(T(V=%qy~Z;WN8f)DPo&-0AKUsPsQ_-YbW57l6B_asn{v8BR<BK`X!}=JWckG6"
    "iV4C@_BUp|6sZ+LL?{HN=5$t!RKaxN_hK!jj;Y9oq=R1pOK~oq<G{S+xoJM>wsYwPpXoY+t?==TC^-Jg;z-jBE!_7$K9YXaMf<uQ"
    "O;}WzYv{1z)S|5VLN#|bKZsmdIu;6SNdyM9FT-tb7BRCOA3M4s`Mis<ho(i9moj)UvBeyEt1lyKEm*JQu1+pjG#IWR#-YceXV9Rz"
    ";)l+65l!u(@vUnb0(6ae(?a{k67nfU+|s<jclkz3ViHZkw^PVgc&zJmPp554Vn4p};?|0GT*P2SGk8=zd!qqFnYjtGMe4Nmx!}hx"
    "aI$u%OL5U}>|(E2koTp`G?`vQE5OICU5v=@)?-@l1%DWUt_JO~GZyh&@Ea}30{3*dp`7X?S^zp!11VhGow4-6#nR}Q;C76oL765q"
    "FSC@1X+jekYR}u}@+Cy;7_{V_3irj#G+GM|Jg#pz7!gm{yzYPy5o0D{39mIKl$Y?G@CIZ6dC=F2HzPXwjw>4UO|MK;pJCX@@YUdY"
    "m@AoJ)<3qyR(aPmP*l3|h<HpI%O#=Jm-3eu{MVh3C>eJqbXgV}<kO%VY2++azm(^zZNL7$m2^&k2;B86)S^N}-ZrB>t9<oi`g&R3"
    "QyUf?ZVU(;#Iy4fN%IX|-;92;>!}{63Y)nD(rK+CA2da~fH9tc*K0xdS$So;jX;XZI>utDPIqF%R1Y1>p+H(^6A(ZwE(=&?hgA#;"
    ")Cx~R#=^zuo<0}+&^{f%4CorsB}61Js-IN8vqc`&pzj@-2hLc$7Z6U!IHY><5fMz=8xxc5x!|Yk9~loK=hHG?@h0*vF~ZZth>`h("
    "zhQNopxNH%rhVOUx2CPVC`I>xcj9ZoZ}LJpARlK-5h(8TDwj5C3eV_h3($+HHn&<j6J34T)B8sFRQ}ux9Ubb`^<{sq1;g!A6gR-S"
    "z@gj=T@c<C8&?__*&ePx7>B|<yJ<%{I&Fgj!wN{(&Z_VV)Op<~)Q>??Z1?UMz;!pV=}3tFWg#^3)$|xm(W1QYw-b`FQydg8Q+-GM"
    "6I`Ac|86#|@k*WKl(<lsaVZ?zjB*U^f=sJw`;$kyc5T<;namC_!Naan+S1SVG}?GCJojAi9>h!!AyUoDknN*rh61S|TSn46Z}Fza"
    "z$jXg%2PRpL@n2N9O!B0O&E@v=Ym)2gsxdS&XO{$aLYWk-=WheaI+|FH%86*jQR^Z1W~#=qHu_(2NV;Ip3{B-w<{rWswK#ohIwx3"
    "!4Xa<!*Y3FMIKDR9=R*D8mz-~ZuFz@zwLVXR5JkCJ}pG_<u1<`Bs-oZO<P9iS{YTC5T!IGgtu9Rs|HV*l*%oG7_?p&9Uz6_+J@4|"
    "77ThW_?-)K;_(R<E8NWBZNb5|jZQV4WErEK#SkLeeu{eQOs%2etYo?LA2-b!`59{RH<1K_MmMUZ-8UIx5F=-yVObPO*fb*y88eO^"
    "0+U>#24Pl&Rz!xz)In5^ei5kpRrgcSuB&Ir3z?DB={6ZOuxk6JaJ1uG!QTR1+HAF?71ORw2y=<I1x9uRa-PxqEAH#F7f+XjUr<5q"
    "O+8<tN50Xbn<~Cy-Oy2(Ez%~=7<^t0QtnMOor-PuFRz4f)edOEj?7#!W;pv2RuOVUf<rIOMa-mD<d#B7-{@}^6lyXMpO$HOj$VQ`"
    "RCq0D@1%fmbUp{)m@^Py8^&B&;Vb72TcRbDIy*{M%TwVGczHm83${r@dUm`Q{B){Ltp;eSvzNLj6(ag24v5{0fG?di6dtu{HY7YD"
    "kzKJWCg2Dcc(;Xxo(q=G4Wb~!qgZMx#9F_62}^8hXtdNTOcyTj>5USL1OU&~wYv68(LT%C0OH~=j1i2x3+hxH2lQ1-IQ<?C{n>?E"
    "sAyna+~T!S6c=Vy`n0&Di$M;f2tS%gc?nSm__d(F_a!(lGb&HzjJ8Kb<fVh>nAa{dC~c&K@V8)1M1?Y>y|yi@kAV3Is$+<}z83r%"
    "_qi)Qs=JCQOn59{y|C`qadAMXMJdC2TL#Ka1DS4Yb+Khuf)VH^f!G(lpmY3trQGpx19h5&@(4QV%xyB)D1h(iP8fQTS$jqKkMTKe"
    "PE7R{woi>-<@6eb2<ES}PpyS|Tj=QIO$(t_3Lc^W9W4Z;Mny}lklONHe_1J47>9Tk0}fYPZ$7+6giwXMJ?6EXHpQuYw|m1jWT2-="
    "(V!w^Xi#JxNr)<T<V~^yIoj9w=|XexYk4ks$GUaie};h!Fo!fKDYn+P8`k8LG?v+M3V*MG`KbVZady{qn)`8Sh$1xgRjLVhteXLv"
    "0G$z<SX>sO(}dpyg67*m92~=*ddf}Lf|7F~5+qBKMyUo<li9{=!8_)a&=cXd1T&^h?L}N#{K%nmrZ|FluyE>D1nY|{DU8_!e+61S"
    "V^K)t=)1pm>A!}a#cY5WC3oZ~JKY!|2psC)Gqsbo`M2^dqe>Y~wSlEcR<^=6Njo~z3gF6f!P^FseWuA=f0RQW$D>By1qks6bmFO^"
    "z517qW6ty{VU$>O{*MuFRvu&0LwW4A;D`0A5?(lSW!Wn2md@q+WbP0}ht_7<crVdLj}mEQrq!5@nG1>|s2lAzqj_$cukR#k#~cYh"
    "vz9*AIwFNge50-BbUqk%q8K9x>COcgjaY*BUEyVo<FbhnrF$-TTY0L(++F#JF%z*}oey?_6C+zp3(zRfM6$OED9tbq*>PPL<1RQO"
    "3%4pl^yfAWS0t8JBgQ=xtU6^jT80IV#EYiVepy=qs7&0&w6PGT7Xn$X3|nS)K_{v83MYQ8`1wy@;z{(j69cV)Q!hR*IRH6BRRFMJ"
    "_JTC@2Zt^0D2S{htCMQ=?2Kcf1Mxf;Ux6>vXvXb&>Q%=9W#kA>vVh;|aM2nj1-fUkcsBT!ilqkHXv6U<7PS;8dw#^P$=p+X)xoC{"
    "v$ge-#r1q}`I(}EOn103&87I}P<5z7+m(qxz1gjE8XDT%FES(e@{K^zc}Ne4+alFlZ&0B;P>0e2eWF_<$SoSL_dM~`%Lreu&ZR<8"
    "8reTHlk`0JU+)k`Nsi#nUEs#Ubz*V1+D2?z`CiZw7nIhgP9cW5hVjA9cg5kra)Bz;`Fz4lX6n)WJNyuH(x3Tok8Ps9g_Mj#;CE8*"
    "bH^B60mulAbEn8HCqtf!z4p1_oxKnSvSeWHeMagLl@{j&)9B5|#uH_9Z6mr-lpq3<5omxj!fJ~C!$Jw%zE{k*^%Er_feh|+<H@%;"
    "E?Au4i;g0*4~haiS>-+~=i)rKnF@-9z~)A4c&WS=Eav(ki}KQvfwUjzEG2|^G8=W0>b!GeSlwfUV$Xbu=m#Y-nk2;DbaZkp&tvhS"
    "8fV1W?QvjiB2t()E9E5-Fm3#{Fpv!*_cINqOc*y2rz?vl7sPTW*E7EP_eU&$&|8q#GH=O@cMQE$E+mB9L{pANF*h*gScol#h!oU("
    "X^bYK*xUHWqr%&3!Fmm#va#6M>un<1Qt^5e6cn6UDTq2LOKfe_I(^(i8C$%rr)hgh3K_Jah_8v3zC!7pU8hCxNXIngx=OhjL6Pjm"
    "#zaV&Y{Sr|DOzVGWbLn{&0LSqS4()n)~wx}Ufly)@I$6GA;+qOh{FzYaE#xGjfSC|s8AldPD`1l=rtBbYSH0EZ)R#0ZDQZK>wKpC"
    "zhQZdf$Wj@MlP)jJ7~w?-5HDi^Jv@UqiSF`jdcpQwDwW&&lV4*tQ4lB@oK0G?qf6A{3y51x!)n?iR4gX5EqhbJD6~|MQ`%%|F}MD"
    "Ckro|=0TBj)bMm*)OxpN(ELo08v+?3m@{WZX1aFvWL6C=6CDZV+TxjnLdTlc41K`ai$g0&Kr=lTyjFAA79k4X&DN(2;($rHwj=5w"
    "IHw519~lH*1hsYEsmg@5N5gc>(o~#;UF#D>xz9LT2v97H@+D#h(e3I`R4|IoLC>Iv(`PNfjYVf7n~7<1ar~7Tm0v<5+zr&{f?uFv"
    "UJ_Yhx`6tiMsM{5LboH98W9&uOsy*zP23-VS9;u}^SE_nf&h=9(e!yLJ`7bb)8pR4(0U{s+AV4r8T%Th2B_j8->Nk#SG2;0YnDWX"
    "OwfZ?S2w9Pyw;1?=f*QgDMaIID+JXd=wv(nI!FVcQArc{Z{%@j$_YZlAXbZbb3p51xB7g*bd9r1W!6vpP+!D`^9MPM;ildt>{yUk"
    "XqPf|@-+HVrEbWJuT1xcb@Vj_JD)4MuM&k-HRqoBfFQWCnB1bZ!=XBfab;~fDT+DD9$M8Z7mcaix+X&&R7)vwJQw`RmpG;bU~pGx"
    "##~dl0>>M8ylr|o{WiUBeduz0=S&!Q!a6jYMOe@8SaZFAx65}1IsTBo(Fxqtmzlfudd%ZPq)n*ksx6ep^rI--cIM>z)=?0R+ydSv"
    "i>TOFcJiOHs34Ol7|nE!spo;rkRsU>M%$B1bD-7mzIRL+^a8}?7@lx~lZ)mP3fQx}<`Tl+l=Yd1Vb)_-feIx<(Y|4(sG5K=-Uw5h"
    "!y+jbBV9I1$#|5aHiuiGHWH1IzC9NVcR<J=hYi%dxVYO8h0vO~uwdB`nbt#d>!V$}&m2DnHzDs7_1Q%;ERL)_zZSgh7>>SR%+?#M"
    "X@OS4wDk;g1+TQTAXP>LQMi{X(YBPr@s?I*(jjzoraa4+{wk|@9STC*#04TGr=6NItryGsVJ8=PwdzTnQVBBVdhszSBcs7T$&`q!"
    "m{$oJ|Jr}jVW<fRm6(MUK@w`G!5e2gk<7frx)*g3`Xj{q#}v@2(J$;u^m}NIGK#9@x!wn%^oK<HO(>IMy|gl(xVte|t^lLFG%*cx"
    "LT$*W>L{S2Bv7K?(k+dJVngj^S%MnU)j3gmLfaeV9|7DbW){%5tn|`r|M3Fo_@LLUSEhyqkoB2;<Ys86Idy8s3zhmW9SY~hc*eRT"
    "A+6#Ltk0?)qCZOzdDua(?3&$k@U1rH@v&XP7Un=M!90u5_jARssxRW>VcdNQ@}kJ5eTN=-h7aNbENluwoZ^OCfX*?!AesuKr1!Pe"
    "?HLn%EqLz_bu8lP9Z+1RgNQ0jR!(h*j&z3BP|QKtydxn!1(yxcwo=!t6{qRF{8M*CEB;M+F{=s?#`9c+S_El(rn~eo+@DpmJ&w)_"
    "H|Y8b@&ti69j9sZWfzv>dg0GUypr(RrIisVN6SpbjZqJ*EKiG2NBMd4x%9_(++A@`5N2g2!VlbF^|}{QU@uVPzn&WEr4F1}v91=R"
    "DptFoccm?zwJ^lCJ#!Mdm@YUl1fAl}2ac9TGR*^loF~Y*gXAH`pZ$bUZf#hxDDQ~xb`fJVX$YWrM=O{ofpeR<6`_pSI<B^QTtj<n"
    "e}<ZJ{g@2x-n4%-y&lh%masU!+)nuwEwi1QEm%DzI@<zbD?%T&p-E7V+<7zhx!|3%7DPSv?k#5%I`-8{a472Ug^pOu3SZn$M63~-"
    "puQF1t~d$`pfiA7^j|FV8EdZFx;r}v*<}4pIubL}#texobkp`BG*<+{xtAjL;Q*`G%+R2cGBX}JyX_^=9)i2BomD9xM~|%Ulyt;x"
    "w~_L}iVB^X?7>9W@xd_|FSjkAhc`n-T1D)6S7a)m3*H_cZ8AJG5l3A5bV$&|HuV*aj$)=WUew7726UYYyvcoQqi2h#a_IP$aQXCH"
    "?^gA7l<+}CxV+AV(0f?=nqiHNjJ{~+z_xmABYfz^VmghIIT}WgGj@ogAo~?)!q@$gA_gm9{fSPUfWMHwJqi<jX%Q3j?!^3+kQcIU"
    "mR>tCaf^122nfQ%@WfZ33177|6dX{PwZtG6L!IW7sD#y+02GfvP!+TCifmqhW9alp)H{@}v4`W1EWQR3;1i!SLrnqKPLwcw8&ilk"
    "Y6Dum&p7d76y@HrMMPQkG^W$AAzY{sqc^RSYK!9UYcP<3&QFfE{(3RGB92a6uPY1PC`=!_?kX6QP(U1ZA%MFlJ_VjrrHrxCU~I8q"
    "obp`oyH-Cl<pnac1T*<$HSeOwK0?zvnBgRYC6GE!Wxp!saWNXv$ZV6o4MFhdEB)xx@%s&o=2HvO3vN4g6G4YX><?0`-4eBrXp80="
    "Az}ubsm(J{j@Pxz&x9Yo`mA7IK7Pye<n^YggP1RB{d9TjZ{?o)*2fLIh&rKM>x6(sdKC6jQnKhfXd^uz@T#0tRQu^=PmHJp<=TOy"
    "W=kEFs3=fc6i_#l&k2<d3|ZY6gJIUHAk(=>%r7G!Hr#i8DEiDXtPI3h<ldEoNP;?$o1r!9{XR{nfx`3P?qp-dS|hy;-q>SSQ=SWc"
    "Gai|WAa`9Jt4xP%8n#Zcz-ts-$aaX;^+4+tIyxewO+>NKT%hgCZJQ4-0vzof2u|23A~!@2m%b906xKC{<T0xbT$woB>zhZYf(~ph"
    "_0jW3_sa&HC|~aNB=hgwE0of|qnyMA<y7`#7-ot=3DbmAOB*yF)7Pi}-%O)RLr_5cX~d2*4lNYV1;1BXl+x&<^EwD9>?kP-Xm{=+"
    "N=GCW-DzdOf*E9(f~qL0pe@sdiH{8H3gtD46ymj(b&53fmvS382Lh%Th}eSIVAp5WpQRjG1@1*F+IF)qCHdYFdFvJxAkmw+U)eB*"
    "Z{!*GTnO;y`d2|28QN8X<K#1Y#29WxP+z;a9|3d2%n2ZXt16^XHj-A)1@AO46A83?;&;3SmYfs3)=m)Bv<|xg<iq#Th4Y|bqzUNY"
    "6HCr0IE}R6;jaa6B?@kVVw4Ur)$}M#gA$1c-(%tJP>c{PR2mWJ7o<mLEMgf|=SZEQVT9eD$Ko1K9F!Q2X?E{57{u^!8r7Z`>*yR2"
    "=7=h?U$%w1R~Oty-dlh`Kg?~R+1y^S^lxlED2UL|y0s!VzRT&57kZL)s71@VzGrbA7=Vqp<LW|3M=vt8q)+Y90SPbPAk4YG1v&Y2"
    "C<UEyhp~o&s6YU9=r;GVNr|s!CIc~hV!8w2TW+@XoO=imD=I!qZ82X#rTQCv%0?$2|DJ#hqmqN-fLDZJid`EY`<c+j;Tc@!3(e6M"
    "U65(UBKf&s`BU7bSB~+35Kv+iP+J=<^jf(QrIVIh1o*_Ep;<|q>D^TXvT747wqvjMJovY>yif*%9`<S!wFIV9Jhpbd4y4qya!%+A"
    "VF03y$$gW~B>Lj!SeIum{N~G-K`VY${Y(y~(Yq|hr;?C(!xLBO{Fnhx+DfM3tf6p?<Zfd~fc6N6o%KFxWq!TWWNzt8L6asdc72^Q"
    "Afrt~9)2u6G50Ivn~w%Q4ij2`kv6)LN<^_P;whS+DgWvKL_`bVqg0$|8N+qVo=J$IKh!JPF?uzJUa~UBqQ3-eits=6%aO606!Cs8"
    "c+IA;ga5S(l5&;e=UgIWIw#&?EH5}OO6Dk8n4>+dmzcGMaVwpD1h`ComM_y+FQsAD=7UQ`3k8FJ@}lCHSz!x#&DfN&val#oj=E68"
    "J;^dVdg36Rn(3ve6t3`q0*ZhbPTgE9(J;5QM+D!jf)2_qqhmEu7}*gx6_mj;8#CE%^FTarEkU33ir(pKZ?RM^2&RhIf=E^H9+_%v"
    "ogrnSQYMKfD(O<GNM>mP1dgzO=E!qXqi=7YPxvVvM1_vysv{&;loOUxgHLU6BE7fbOHog);3;yr3}i;XYU(Y!Q9NfvKx!iD_*~Fm"
    "k7r}>WpE<z`Yc(`;f~au7dTA{l%Q{RtT6UucsqzsqEka#7$iecKrAvp7rg8#GAx41iC7VC*x2w2+;|;|j6Q5asQCC9waV+n?7|qD"
    "7!_K@O9kVV^xg9j|0UFHb72Tk$9JU3{GZXe7S9*n;qC+zmK0Yj*pqAohd1pz1Eu@kC*6=hx)-cTSE(8EFscfE8Ier9>tewtL6C{M"
    "0)f;TabAaQCzf#N@C3A!70RJb8O4_;%TM1ZlEc`B@LebvxDo}~xWRHmiGL~3X<PAxVrV$qVSlx)A2``+dv4_OSp3ymPILEnk?-&Y"
    "&=YoS5|mwg66f|nj66Mk;AondYPukIIzu}Rek^pSt!kq|Z&h#qx!_m!<`&Ol0i!=+E8PseB0UPePt!8WA!wDw%d&uP6G3Zuw2GHb"
    "y?8}iu$GX<^Amn_hT*p}n!9NF*NJf2rOZxla{ItV7jx@CmEX-xFM&QnLFXl7F^@^8qFZ=<!u%QcB_yV4{!lopP^cBLT2(4X7G1zw"
    "5QMQg(c&|ePzCIT-X=vw1$tc4nC^N_=rN9aMHGJuxKKcmvL+so6ur4@pGTw)Rrro2sX=i?;Cpe~p1Lq(RW9U6CVh|Bg11vaY%SMw"
    "b3sEH_2H#JDa1{cO{6U8X%3R0@kKZTJu`~lqLHYJcCjlR-Ip#UJpNvll(T7=yYQ#iiJu<OE}R%lM2i}{=`NnWon~p?SEe6yvRacf"
    "R~qq&G4gl$gd*e-y%%5~IOI4b4I%9A8P{<L!i$NBra5FzFh5*qN{F9N#3PiEaWE?H1^oqzI_lKE!azvDQYHxoOF1zYFE%0ICWJ^~"
    "T_HtWBx#rSwHt2|N!PA0l-5gIG~{b#o88YMMIT7Oi4pjgJC_Mf=P-%06yb2aU=b&=Vlk+oZH1RfWq=^*EcGSMmYxe<YleKR!SxYg"
    "_Q?v??0tk{t8kLRyc)Y%A&ot;F0K||IWZ7xK|O*4rdfm+Un_p2R!rj=O0)$P+DKti`!(&!<%{y4MYv4~s{v`YPx~l>1Uma2jwu#c"
    "f(i9&!E4<|o$w5kbrgiTO5(n3G5xFK6?z#o1PS{Li~*IA`O?8*q)<r~_<U5K?Q_A~{t+2pL&bQ~uG*&okuevZ%P$60?7+1n=O}uq"
    "G50`sup@EkRcrwS#pCr{@QNrZHIgj^W)xN`8lpj4Mz}`QL0NcjuLLo$6ltZ=C|stvWTkUH%_xHJx3KZ+8<r1yGgNr25~A<WhNT#m"
    "brQ?LOf=M$jq{|T)0WZX7#&4t=Bq;yj}xd$2oJm#bXN!yW`bIj!gUE#NH9kxI0+8b!v&420>fb`Lm#ud#Zk35(hJ({c~o)wbG@q-"
    "7eyZssp(j`71TFM2^32hy5|p8nx{NcJ1z4%C$4NoTUv&a)y_5aYS;W+uv{T(rpfRHS8&`Yo9a*OQ!h-!mpVTa8EIQ#j;Z4jJ5DU3"
    "tAann20Eis_wLy;R6Zn3|Jboe;l{*Z3CLv+>N>*1beU6Nb4=?6BrWvmT~jozdil-ub%wF^M)&gxFHAJ0qK3+~^GGN)>lot5jfJc-"
    "Jc8GXnb9f_>BkATx+%I=xLf0Pv5F$+reys(hgVoZ|9p<jBN*EAZA5@dX(=;UEWAji8S1a^2^7zclM;Gdcwac~0ep#C@i~uBzKew8"
    "*=cZ-Z`8B(r*B2)fNV2L=xZYX8IFla!=|xq5j-nUvvDroE-Cim0Q6e#i){L7f;F^z27P>;fUftmL=N@aTcVAyL)VZ!@z7PeTxUa$"
    ")}nz)f6Gx-dY=2Q>K!W3TgRdbtO(#vav>@zG<5@|Xad!tz|2`oG#ckd!z(wS8jo{<5>%|uC;VV(poxvn^2N}kz(n7D96>gYGs4KC"
    "ZuCM`Os}Zg(zGOEII;3puyCRkn4>)wte50-0evWGRDumlS+HQJrSo_MSwl0^;Z#PQb2sL38U{C$D9_jnIvQIf;e|2kC!UHBNafb~"
    "^sEH#TC;`AEXT!9PQ6Zxfu;WQ2;I3O1R#*E+)ol4T&XbC`CRad?2GCimfv*_Mg9~%1*Z#5bz@p_F^kP=)nYv$sKoSmLXIa|#{WAS"
    "<mPEaE+F{bAR;YF**ns2JOz8v7&cuQAvb1@8{N<Jv~UfhQ%`?#`&r1-;GOiswUJ*AnurBll>-CvUaJamBIPxh`iqKpBH&5qHFn{A"
    "d~X!S1xT6lw94^Ff1+5?W18kUX;nYers$|wN+(@p@&X~jt)Cq|?oOraed`Y;!Xl)SJ94flrg<Pbj^5K2oQvmkmwu?tMlKK``S{6k"
    "{}dXX1Ui7)kuBSeaI2yN0)Uuoxk+Di>9lDls?v&mA{YDlh+mzZiUAT1`iO!{V~HYJg7%TJ4`RR-dv(3kOsi+(f*bKfEZAu7ZOWlR"
    "k87QIu6XS(ES>EfMEz-{F$+Xzr%nigRTx-_9tX(C)rpdEm}qq<kSE^xaI;KWe4YpKLs7i2W14NLZ&|0^;RkY)B`6?GWy^-@Smb^?"
    "a``Q=um4@1P(KP2I{JI*+5S<#u^Ed}EV4Km35irGnF<)r6#3!eOX53|dp-168@DofXF9X$%ueUuBl{zi_k6;yvL9<L6?Rd8DL9i9"
    "eYGmUPp^s6-Z=WmqKzUOJu)RMLWOb_O({Y;dQ67~#B0HFTfK`A7`HRQ%@mG^g1+BMKXfrE3R|DI)n=?r%rj7<jfGTlWgKeZCm-yq"
    "WLm$D8sR2UYp_0Ft9eoFgiaF9a>~U`M?lF4A!%hkOyB7a1K}vj`?j(Ig)2m*=Yn^74K@4IQp1!J7k5kWl#W(GU^+t!v%(Sc9{rQY"
    "bsSn#6=n5w`X3tUx$XboE5<t_FC4{drLdqw#W@VeD;6(heNc47>n3C6u7?-hT}4qL<3j9|hdDqB6D_|)g~i0aArFhg3jOo?SUPxG"
    "Oo!Xf&={gFp7|^9bzq{nT+`Bs`!oL5oOUPk^z@<3SJ5>74dn+E6wy>k!6Qp;$6$_JN1<zjiV!dTXJORTe52i5KSz9x;&^Fvhj>h("
    ";(l3!YsGq{=>qz?bSy-sfl5!+DOx6K=mMRhs+;lVk--YR9tSLCjD?ou3QK@7>eal22QYg4DtyTpN((|R%s;SO+&WFr5g~#mXw6b1"
    "i=cHho0ye%G$jWR2U<j<d-HrmccE0$Rw01vQIw;)QR<7m;W<1owA`?lDQqQv-CfVCiZEiZP|tvSLM<cY*KKz+U-5*l*hI8p046T0"
    "Y_W9&I*RNfZbREcCj(pi*6|(|rG$re#UFl?n<C^^P{F<yjCUBx3iHzZmA$eZ!@WorGvVN3<mZd8C;~)m3Y4&X>ZT`4xD%vW(}Oew"
    ";GX+5e$fW|DNdpeu{VrZux%%T@SDcDEy{h|S{x8dN7B-{Bg|S9=LJWUpS6d>d_Ljr<4MWkeU3w?fDK8Aw}kx*C<9UbqSV!9UB-vL"
    "bQX$$>q=)50kfet-BVaJUUzo}a?X{P!6>YC7x3#;QdgM8F9o$Mt=hIwh(QZld%21F7NHI$n(md<CUWP`1?x3}fols4bD(=lr^p>d"
    "BdQg7q}(L5-%1lE2n>H9kfpT*B6C4&z3ZuGdEFvLb5$_2vB!LM(|$tlHP+;=WvpVBIO-Pno?H7#{}Try6__&xEU9#U#lC=cqdm|4"
    "O&B`VzZ6BIc<OkUjDVSj*u;w5NLCC3nlKOA(1(ROOc)g?=+bEV;ED*ZYea@ezcQ^0NNmtpV^L&f7=H(Zbz96#|0U!&nGnFo-G*7U"
    "YXfp;jgvY$-meO%ua|1SVun+CJZ?UbOIREu#f^0O>Qs1fI68}2(G{V<Q(P4Pj$O&j4@LN?pIf-Lsd_KZ1-~k0VuqY<tP%{3Gp*`l"
    "F+@#`0-`2FP9=Y9Qx?)lo+A$a6a52p1{gEytF)j*_+76frCqU0(C&}uuH$66S|GrT5aO99*;yaUp#Oaw-V5=;h)uO-jy|RF`}s~k"
    "J#lUOpIriu40;Y!yYuQ+u`5i)X-jVcCa(i0x;QeT-$h%+BF5{JU<39%J-rtEu$#+~iVUK{tB0gN2yysOIrI!3X~!bdK0OjY`kbp8"
    "x5ZW!xs|CqVTg(L7nIV1w@HFYh|>;D6?0+M(Dmo-6$`>2L+otnOr})1K9~kYATV4xGlNzStYl{9dOzYFc}5b6KFkfL9s(;mHrhj*"
    "SO6QJ8#7I1ccDi+NV;N8MKCc6Tm^kq43h10!FoOHE!YGmtmKdoQq*oK3p<~dtPOjuW2S;?2JW4SYoL&PV<=M_w^ae0@M@1zytp!B"
    "I&8;Y=S)mv5IoS%27s1HDXK73g0EPhj#<{U5vR6@U#NDL9cV3;7s_y7!#CB~-wlTN$=Fs=#GwB%rpQi1{F3P%<qj$VwCv&Ft%p-w"
    "IQAd<=|V$Tl|EPeE`>tO$FCb9j!05hE)qkU=zAECMK9sIF*T=8fdcOovRY^#GT+{`$y*rh`A+YIP%`PK;H34A<>5l#Q(RwJMM-uH"
    "kpf&iy`D*QxFRGx0xjUgz|!YxxO%$Rg4gTW+Z1W46DkS^H8v5owQ&Y3;V6rT0OC<Jw^&3gq2x9c++w7b(n4$5Jj;CX8WTIbCTPe~"
    "1nHcx2%r(5LWi>GX(o;q+C}5n5jpofE~4%f1-inxcj6lEz2H|HCe|D`wu$Yx(xG->|EV>_D26);Z1kDwKN_()+x69lQl`&hruwlM"
    "vW6%6cAwmkXNdfaKn3B|L0vx~+ye72M_pfg#~n40NzO&w2s=x$#t>JhvdUQS>rO}uUhe{GQvQVRGE!-kTVW+I)GdvevEY&iqA?t="
    "$DxY{O&L5Sypq3yP-6D+g}z<=>*WzkMId%ehH;_@1j@)lN+`VOC#+s+!ceX<6Y+9u>G@KC<4%Pxkb$(rcrN&T!w~hukA8$eVbHk}"
    "BMg8EJjhT@RXm`N{k*$Eiy|^3`gyU`V=$Vht!=!p;`KomOe^6Gm*mw}q%lvq0&rn%S15IubY=x^Vf@rlPeYoH@m7Y%nOa;4e(;(?"
    "j(32SO3xWWrh~^1M$`lX7MGaXj3w<(WmA9W%(W{qX4J+xKk3wOE#j@{W4#vqdax;7cxD3?_z^?*oXpti<ef-vCTh0}$K|a}3_0x+"
    "dx2Va7tK>bo^&g}2IsWk7fc1%BT8eKz%<4Hs)&QwsOx@eYuJQb>0nM)FaVK!%n=l2L<De%H7Hl>*uEHscef>4(Czdog2Y2Xdnzck"
    "b~o@`s-e1E8CIeWAF$F(-Z4~$I3EsVlOp4IS&~P8Th1`Msa)-K@gc_2>6#TW+oCkwxy3wz#<4QQ=_kH)%y)}0BkVF7m()V4FRvS|"
    "__Zi|8uHMFMhXHYwnBF`X5ne9)#zfw)so^q@428ytDGB}FmP6$QizXR+w8UAt(=Ob|Mb1l_6iPbo#Hz;1)|w=J1cw^yF$)P0v7t>"
    "{ah(1SBZZM=`Xrm!xMh{eBY^~3s+z9T!9X+Ovhf`IJ+ue8Rikh<Jt}i(=O1JysHB391Km`CbqM5G@kL>e|kJvtem?<%nrMb8nQo;"
    "ERM8LnZmzOAR;0S;*y9xcdFAh3ez6UA57T*;^%_@gwskZ-Wl`K9-l^8UK>HIiRO=(BLg*iQFC&gJ7Bl8u^e7ZjUt(#E1EgRKZDzK"
    "4aCfc0oJyowj*ui^wb)`(XqqcbeYtaafQTiRr^HASTJAASUA0=nGzH1&my1yTHFePitN(XjlfIyUJ2ox2hnRK*&w_)H+FqRuV5lw"
    "v2YeNqX+%-%3KV<)#;xL=1&OoTyZM}D+!L3Exq3fEkR53DQrndd%H1u!w+4drsGz~uz)ZvnmmG1<^S=7<&)u}>5?+0RlJ<Z>do$R"
    "t=?!}%SFe%GqBU>I9a_BMngB1SuMq(wu)%DdG*!^QCw9Hp%~P7GA54VN}=oQ2>>d}Egu>ub6X6`&?G6;pKU2cVRz7$?dctgMOoyz"
    ";CJu^Obxd}JK}-nCaQ6DRc+F!KYqG|W2mw+X_ux@KX3>=9h#>MNxm5SWm*!$U!hjm>j*XLG*9sS#;-|f9jWP{)nnG76Y$?hvRX(z"
    "8vdHm^9jFp=PLc*@%f1M&q_eC8Omltn#F9qLomuibCfnn%EDr13I(n8PK#6#VnBYgaA8I4-)Ks|7W_I-vi^WXR~=)C_{`}YHUzOK"
    "_*!2^N6dy6o~<{>7sH4pPQe_%Fx65J)OQ``x!^}HglP($AljMYOSm{zz2}9DTU_21oOP^i4CW4uE)&xN3yWfAmPQmeX?=}X^u80N"
    "_2{M0W===M(!Yd`7-Ye`gn4SzikFUmv<^X0#c~(7xcMIRyk@_}sLHeVQw!eN1do~>A`$|jyLZK^W*Dg6TaZqPLq|YFwO8C_2FeSC"
    "Zdy1pAX*qXx+8@6TJaO)$;ib4G7wfll3rU!BtI}8)HYIlr~^8^V1y~R-aEX!X1XOpR^~#LxcL2CuU@lfeL`q=PfR~xG>o<x0}sj@"
    "R)9RAEHZV-x!o6?LXBewVHwIsHW`em`g50Fvu7trLkspCUFkgt_KFD_A^fUjr{q5cuTOVUV~j^wD;ZiLJ(UkXEp-VrSD&%%>v22U"
    "gpgomZ&BSd7YjE+OBmNHyQ%bx&j%6XrNE>wYPiJ5<DIUt#dSn}$q&Tq_KezKW<g)@t=5M#sNV=tRphnB_FEAiH>T6;$uub`LPr<r"
    "Pi+?;S2MxB_ssRUqV0v0PY;G5yfwq4o{g8rN=au-`A#V-3kD?0mq2H+WTkw)cRqR0sdfI95W}rTp&`Mk(DvZ&X9S$mbllrCXzt>u"
    "zD{dCw55UEl&447+Eu2(IlP(h+Sh_#?K?*#VLjET$_`iZ#qe0PcN%J)jf$VP;^K5$rDy>PK{r7~MIVDzu+zqhPwXAP{8WlL`U*m~"
    "6qQ3w*e-GFYI<(CYk{)GX_f#Sw|Ew*e>}CcXOw)=CQa{*AHR-h9cY9F6iUZ}x7XfiIosSsDe&l#6gssz_ytM7Z7ZrVEr^o?>Q%Aq"
    "G4VBgE*Nhk2v<O~nu;s#yr+kNwd0+QhZR!^ex|~Wzin3R1;gwll;}}*8EBi1^4v3bg!O3bBR@c^5Q0~Z@qo$D8g=(roM3UclpC1b"
    "%LC+Vavwx+gKxIzM|=8t#Q3E{5rtZm@3D>4za0?{%CKV8-pvLF%}t9?XVP+F9ASh$xaL+&2hOx=&?<SZm#=o4;uqP_w-N^eiqi~X"
    "+zQxto8y7NNTt6Xp;|AfwuSjTfiGIrWCo&uuLVDe-{81b@Yoihs>uw;M)XFcNZ2iup)GjgcJr$v0n9LTHUi-&5amWMy^i7Y*7Euw"
    "BMXoy><iR;k!bPHpm&-PCl*G^*i0*)DG4*RNLJQwWv--E9nKO7-o6Q(JQw`>zo6}go9BY*wjNk?y7k|DEUo1>HspL2X<(#^4K>e="
    "wwU;VVmqF`T&X;d;Vrgd{DQ0sK98Pmtg2U*!kN7lQ|*d+j9TMMZqSy{R&{CkUKr9yi%*W3)MLH$bx7!(Wp|o)6IyqIGks7p6p#`8"
    "V=if8kk+@5Pi!VGK`C*IQ9JF)h^ppuS?iw*{sU+#YHi#j2+xjEaW6Bt+ENx}T2%>Y6p`{Ud6(LfICJ+c;+QC#)TZMlptII<!LQ*="
    "zcH{$srMRf1PqoDFtz#%E$1$Hr4^K*!Nfzq1)cU2=9eh&9%s(oJgbSx{SE5bOeCFtDMr_+JlJaUD6uo}Z7KUiuWvS-rCz+}R3g?M"
    "jTnuT1!80T@wwm!N-L6F_{=du;*KV*q9`h+%5_HkwyfpoGDk&S|C_7!tq9Ii24n)2&<am|E%^0AqccXQl0kukhV|B{*61Pk3MkKu"
    "n4!*Y8f};Qqq&R{no}-b%ZP{cY{a*oo78_?jXP+%&BBokbVJBmEFlC-H5Bh1F8DDL^1NI@WV+$YaSJj^`g9K(z1!yl-u~;HX}4gS"
    "lz}d|n2xs>J;QVm9}Fs;d10s5gneSf&fQ|^r(`jjMLn5%!)w9oMP8XtCmFi2(-4Xm2u@2#RlBf=vmkJSmZGJTAs|?zobQ6bov^(U"
    "27O8Zx7UL8hHj&6Kui1a%hov{dsjO5k#d}h*jUV*ux&eof;x6vu#h_ZuKF$dFYGwVbHVaSWOxNl1|Mq#h2?qdyy(61arz?@mnzn-"
    "`hX4pP<=5a6+0L<wOxpg*6){6jlPuoD_%kk>2DM#QqVr*h6OFN1>^K{*r+ea-R16!F-cJv0q1xc9T)<m3Ff)pH&TYC7<X$GojK$Z"
    "w?IqK{={m7l3EhD7~<PZ4j80>a0&N4BtZaeSN)$rJHvCqPxH$viXiv5_8D$A><i1lz23p3x|q2-9ii)pPK9-9aEo?g@WE*$fV`#W"
    "6JGaYv|%xv5foKw2!O*ZTB2lxx(9=t!uX{!29<U8^w2b%MzgV!7)0Twgf*TE-V0Ji<id@t00oLrLNx)~r%Fpf+Cn~~acAgu6OqWw"
    "S}cJ$?G&A!&5W1WV(Ga&7yR~2WDJMtpoI7F67Orh(h$(w4ykquu?PkWiFyi)f&ND%%yH7mTr()<Q+a%?7_L~lwxT;7E~7-o|8SS;"
    "_^orb8-)CmqRYx$fXC4ErjeziITBwx1P_&{x@pe^KLICn(-}lm`d(WA6{EL!jLwi1$bf5|?utn#>Klw8EDWwfuZ-4bgn0M~uLa|M"
    "r-JbL^qEq{o?>W?NtT4WvhNX7fs7(N!iUNh0^UkR8*w=5CvGKuPfV!dbH#WY6cK8sT51@B0sJ0WRJ@;k5%pZyXnMW6)@Xl20DE%-"
    "ge{iUF4M;><4N%IHMcrL#7qR^x&YoAifKhq(R0kOD)F7{(Z)KGOUL!PFzba=AhX@E(@pVOuv}$PV}JMR!kGHC8{4Nf(ZvelxduNn"
    "oT4J8sB<v-zeU4on83SJZv<`s(4PD^*Q?zotcHXl&WB17AVi)vRsoIOI?+Z5Q6ItV-l)jya};ij+N96yxC@HNy<l7UW{@rSFn|nP"
    "M-ZIr)OqnonTl%KnDj*Bl3cl67wcbm*HiPV-LKXt?Gy^NuLVB>oY0D+-=r`Wu$dWI^gGUe{9TaSQs`P;C&CEq<m}t=4bQ_VT}}k5"
    "@xFU0NW|+$Oj8s?)&qSc@rjw?i0BvYg_Y#dAc1C3hY}DLRaIu0g_%K*DV$Df8c*bX?TD4u9rsT7pJ^+B`;jr!OEuNu(2~!D9&>V-"
    "JXPiW(vWm@#P=iyfYYvfzR>G!#T`k~7o<fQ!7@b+J8&f6n#mD}W<#OL9QVVhj~8ic_aaVW+s6rGgG62n-U;N0`w>&?6eIQ05XZBf"
    "p=TX0k?Xz{oPDMzQ_zYbQ$?gm($|*NanTW({@Dm!zS!;fNW|_ov=b0v7Zdc%b_l|tJDEkNDim1XiA(iXsBEM*>v8gP^q*o~^jz@U"
    "qGUnXAp&Y+>9i?kD@owSq$f*m(uJuwrhO#Vi0ymV<LmD!aUr%0_73BDXHot%v5Ig>!dL^dGo{$D@y;58WEqi~Ws&jCYxv{nj8+U<"
    "xJ~$XX&)_mg!Px7uk<Dh@n2zFiY5UE)effK%=)^n@Ws%PSV2fSYKE&fWnpoGWfb5NO1(-T_?AC4B5`oIopeYn>k$Q|(HhbYNL_@l"
    "+O?)|q0)g1da*P=3-fSmpg`ny!j5jhUW*I;S@*h*(iZI>Zn~7~vK4|kYK_Ao4{N3lL(3y_P21?W6jj~_<_VyO-WH_|oCBX@7h3QG"
    "rUjgRM!dYoiO7!V1ZSo=w%2*vA-Ho*5)=Z&_ZNNM^eWRDWsZG#U5xBL7yK-4ES*TOCmNjeXu|Zv7^7{)9jQo<qiAbAU1ow4inPVe"
    "j&F;yQpN7lp1H5*f?uSepE;a+r|w5Zl@3qqg#Lw?lKx#ObU|4RQo=Nzn!+qeIYb{(FE-#i-S=~wexg`GyzI;&HbLN}xTV0NbWxNa"
    "Fjk~vCt|+9q!$KnVU)53Vtwq)mCZG6sMWj%f@8T7RpNRK-Y{1Zw?cD7>vKl8l`iTKzsBOcMD!U)<ce5e81)eyA%h6F;<|9byX)&t"
    "lM01a3)sh*PCG&m!rWLc%*|BM3@oZ0V{C)@x<-T9wp+^#0fpV|>KL58yscvWDp6?IpAMz8e{G2c6T$`9Rmf2^SUiElYmQ>DFh-1x"
    "`Cf%U_9}JM3E6rs__Yc~N~b6cBCP@6(>9<^Vp?x08RglDYZrUR4{Utr4ja#2rNfR@piqDC^#y6_EwLjBdn(>oVT&n=3qp2jq0^g0"
    "Y@yI<)%kX!rkEQDS9IazBpRh)Aj&rCv$^FuSi|5?`~2QLZ*BtZo*p9$+FC^usj*lEXeK$E!5?^uopY6=Y=vmZwCIn&7yR1iZF13`"
    "8McP%RHJ7V1v>OKtzKDUS{;>gTGq8?R}y+Mc`;DFxqhCaxA`TmfI8VX)$O_Gz|8{!Abx0~0CoR4r6@>>{B;p|qW~j55d;i?kZFl!"
    "IHGq_&g$`5FRkBwELsPrF`^D3X~jov7Cfsh%?ElBNYV;poKfSF1q`5iZYsR$g(R+uwSN^oUMQ;m`J=c}el>PaeK|2jCFaQt2IC1r"
    "0os*fx%5JZPR9vuCVLbGggn-!m#FM#x~N~y_hNx2K!H=FJc;_{)|(r(V?tpolWjt=OE6NQdK}#&9d(Sk%|ya^lMwTB(|kFfFJ9R+"
    "Q)#IBGt+7C>eB}@jX7>wx#<%KGzZb&n=KBB7-<R-FV9fDf3|+qbg6~7W<H}xI3FYMFZ6)Hm0-2yI*Q6xs?_m>!b?-;paPl1{1p!j"
    "gf0??o(q0G!iDyxGi-&~3ub&`nJzAF@S0+05N*)I>wIKxDFrEfMo85NbW_Q+3oEAQ1#taTA~91t<;0-$;2cUH4wqpryZx{<e=?Xr"
    "&!rcw5D>;#OYaq9ouAUV534@!N$O`82K;l#xR7Rh=1y5DPmwbr1C0PXbE`+60B@*P#iRWCHPE6aA<}_@^?nBLa)mdUjX|7F!A1MO"
    "Pp+7)@Jd6&VfrjKBZ$g$a-Lp*P#Fsrl2=glQ;4<7VB*HF1^-%h;RkdjA2L|y=~&$^P7!Y%(e1Ixn<6aUGC8cMjs&lg%x>0CWU*^8"
    "p^fJ|{baxLxkEFGHUIm+k<2yxvN-K<UDZ;>sI0Ywn{X7JWGYLHH6tk*1(;nx$WM%XqsLUr&|_(n8g0Am%X7s$y~KPLWn@ecwc!K("
    "n0nP&&<<#g<l&?hLdSUsKNt!fBCb^wJ1aYcEWkO!crEDv7)gmdA$?l-4pAwdaWg`%*ynNh*?lH``E${~#i=c~xp8>6Ck=@r>-D9G"
    "qy@{h+!R=(d86V4DQu!(CEV3_E~^HE)Nx21FDlcm$Se#EVRg|`r{zTpOA}eEuLZxkMFmn+A{7o#CeuN|In<+Uu|^ZaaSP0sxNBAL"
    "<%bMv_#NTi;5O;O2#P&l>BkSeG1N`;O50*0qD)Q@vdbBLERTS65(~mxgyv0Qu8Kl|?Gx>_DoCM|5J!71c!3vY4!{yYgrq-)cBiKA"
    "BY-u8JChD}FO0b{3(bX#_#Of`hU6+7n&@a!LV7Kjum1;9fM@trsROG!EOl)Z9N0ZZkt@uVcVtu(&qhTpQ+DiUlrn=T8XhS2HPweK"
    "(G`87nn2dxN%5l40i;0eaWz_o32MiW!c&I%$x3ybo1ds?EOIAuujA@o1VOmw>ov&AWYp&bPlw%0AF81Zn1tg@r|S(MvQtK0ooQDA"
    "qvNh1kRlisd=R`>e0ZMxa_tk*N~YPMFlCeaIa^kL$>?CvImL~N3!+;C=j)-<t{-KvRwx6~XLqID>$%`J?#jS%i6;Ofzi-7HMX5H@"
    "t)nrdN&m)P7;$9abqp;{w{iPh7#Ir#%^943zSDZme{eH^<SCNGvhga?hn;w-aD!rTiG$uz4+u&>j-n*j#X;eS`(=xh^Xrw`Z%9KB"
    "tQ@<FDC&VNYcZqd76y{XMxRjM4r?5PW3flPUcXC+wT3aoCYa(*R`C55+w2u3q#GKuKcoRdq-$E4p^lgf)^J;okp;{3Xv$-T4v3}|"
    "1(T8jMK`fw@TesJe8P{A_-ybK0w!S0(kS_jm8Ld}{**OQ%H(bU5kE3$;+d3{Y(qG_(-mhp!0!diRSw|7?7#oP|4EFSuso>l)BH8D"
    "-BgTet6^)QR4&*hdM?Dk(75p^Kr+15UiUf4esEoBK4j+47#h?`7R?FybW%nx6*sOpTl9ZAx&Ynu2+CUlgrXn9H1k@1_4zV!(Slcv"
    "!4p5kfLun@TrjGXr7hYjA_+)IXi3<CRhCLy0p4KM2!RmUje}&#oz`o?&*OyRf1yq;zSsiawuI_C{q=_&xFr&<a8^704+5=N;QEdW"
    "QeXPZQO7hfnf_ex^Q*{^6SSnA^C9W&Ahpnq$Xiy9(g9OwUW#6BQ?HECx`t<14ed<4yQQt#6&s!l)|>KkLZ)O}+l3*?>DZJSMU=Lq"
    "K|StBJo()M;=Bq(A_l1`B<kZ@IAJL3h4h%^`M#mBESgBL8F44k6Ya^l3wrod++C@i9ws#Uv4;(t>Udh!;u<%_xMsyp;<?~AVJBos"
    "3?p6W?O9RER&Zfw7)|k}xaD9c+;ZBL=t|YjnRL%@m?_FAi%IJFI*9I@&O)0^F-xJwXyc36<;4|hHrk_}p+ylMh0=nuq7FXymB_Wk"
    "f=rk=5#}+1t*=YKblrA!oW+MgimN20Iwhww#b7LMm_k$y=N#eD;Ui<x8Ce#T3Tb?R4X%d5h#vU4;@5^#@=u01Ovd-{FYP!rFI4!E"
    "D6Oejy>)R-tvtoIL{3@_lg1-UDZOGCRiEp97{jM}t{=2>BOSC`_|JFOy`m;6*%mj>BdDRzi$Kw${YQab2~&hbFsuuHylw-yJH11M"
    "l~!ivBopF$h#WVu9!d^EFAnFnLB=&(h;7z{LT9LeQGDlxj%`!)eswe^_jOQOhnA$HCKz<Yp-+X>sLiC;MVi-B6lHfi%LKCnB^S-l"
    "0;LLYt}~k#U&;aL&MOx|jPjF~oJN7F>Cc<ANGbq{D<`Bt^>UGvulhC0xmCGSdlertwy9pSNL$#i1^vB4s<@XT6-EV~8`@&6#mpqo"
    "4TBz&5U|q3=7lo#Zq8oqc)Pfz^$e0|{kh;3?L#9Z9F=up@1O*Zh-}U+rOAko@b&f+DLro+YF!MY8pwJT+7uoOUMEMi{<Y#4PKny#"
    "i0MSL3OD&(0Y>y+A4th+Z`%CCLB>Q>qv6KjW2muXNXt<%BBnZzuY_?&vO~opiJQ)j#IV9=#M=&$__*n$Ty=5lNDrzQDT2KUJo=ot"
    "SBOud>Z={zpTX^$=@TI}nR__~Y8*~utKyv0QO(G8R+NF2g0ntrO(L#nF_ptWIH)BS6iE7fhMLcRK65R{DACZn@m827_=4F8lfv|K"
    "Cj4-fn+G>bm`UkPB%ums`T{Bi)-%NLe8St7TD`&ry+){fD*K{qn3X(A9|Cf?CoFA3gO#NC1((!Kh|QrgwDJ-d&^~R#t*-^^6|prs"
    "iwMl2t<-d?^)?xv5Swx&h>EZizP@hPDGUkjp}ve(;Xh*AIwbXZF0Qx@;%1Q2M1!d(zvA+%L|<@_s!ah+k;jj!)P?$RT~WWtD;pPu"
    "$WA|8QRK<b1^>(k@WxXL#`+v*3PA=`7r4yWYgcharHZ3?n0w!%2EH<8-;)y)h^SoXP`<{&m>jvr*UskS&aQK<x71fl3h6px#!V&8"
    "i0!1eN>BQ(!d0RdX?KAFEKTVkTUC7H<>Z3>+XvioI>&-Wu;Z03)-G%sdKJ47ZaVfZG&W~?;M-BmniL1ncUMyD+NFZ(>$%`}mn-a-"
    "&UL^Z&_dUW(Jr7^w?iS*q7For+<QT-9EK<keT8UzisMr?6$dlDK3BYbo7Lx|(;F2MX%>b?n^vYWyAEN|gEm8jOGvTV8M$QI!ZJlK"
    "q$hHPaASRSs3rg93q%LAq3)?2s%3N%3f&ZBI;3v&bo#ne+r`sEW|*p-LX~~d5Oe57wum7<7yO=(`}WL9#EO{?P}-?W8tCUJ@nk{#"
    "WlF?`mnqGWs?u<!{b+StMVhId(`Ei#@he{INO^7^s}hwE4rFF?s<)^na^oo=;i+AM@S9Gj7ZgwsRRTZ|CGxVBGxzhIerOmSSx4ul"
    "4jzh8T``rm8;hgn=FJY_ij@;%pb>XY#a8hEtUtNCsuqKp`f60+XI~0+G*_J|+UL`YC{B~<0TKd6j(24)|CHIiLR&UC(u*mgNa^tA"
    "$buZhy^N?+_*3X+OoK)hCm5vwqjQE8A43F+o3-O@K7?%O)utV2M@+{uw}3~GeK;#b_Yw=j=k~T7q@ziwKO<Ae=AvjJKIVkUb-Xuy"
    "nu5FH+y>zyrDsw^KOJ~F`kp}KUgiVo4iO5O86P0}3XZr5A*IUo1H#H3v70rSQNn_|RS}+sQo$BYD?>qw?h1lMY0r21p_+VvgII77"
    "A*Gu?!1hHM{60giddhQCWyDd1HL!;l!owENN2lN?ewtQ#;<oT%i~=U2r$Q-yxE|*TEHw0BAu6QS1I96VHzmYse2NH{PSH4_a1n6@"
    "QstCa?=6CtUm-&qje$3hkc?6d7HtFRRr0=2YQ$V~2KwB!J(g2SHs^Xvq!}4jOaFR&HT-G8^i4})P|Otn&^kH?ifP-OvnHGy={;h+"
    "K=4#CN7R@u^1hGQd$GL<saN>pONk-<xkPHioRVq5V;+}x`~<gI6rt9zI(P}oRwm#TT97-AXF{@WDlL%_dts`VJ$s5DVZ-FAa{u47"
    "QxW2%XCoS&_!K%la>w<azCLW@3`&0b22H6_Z!!;Lq`8rOmTKZh*o4}2{%A3b0$*e35Qu;w`G(h?_&Xl)V%3?x2{BTkjkXmS8&a<w"
    "!Cac-{Yw!%-3c_|vs`|+B}%U-^{$Q2$s~(=)Sd-T1Im&$2(L{%I`l4`STHu@25b7btx()^#ZRaJA++P*<U~f53<i?RXcJ+hi0>>d"
    "1+;L*0En7mSlLwM&(5GoT$vW7Cr*r46D1*@TF>RN0+_(G*tLf?Y=&T5!p}g|U)z-FdCa%XKAKIN_H>b-70{n}g|<Im%INXA!tsys"
    "k{X5NZDy9wMH`wAy<i|n8JZ^59+)J`sbO?Mffskf-E@Fo3&y{iX-dc6h{o^2jp6a65lzw=;DRxji8K04$M@8LE$F$T1c1g-(J}J#"
    "$!6kr<AslxJH@CPl53b@2xe^DV+^k7dsw`iYNs;@GJ~0#Vf0Ap$vA;7g<vs`S9*${1FqFM^b~0=F5zd#E+drB-4-_&RD2!I#S{}!"
    "%H*stk0Sc3*t<NlX~@hy7yM@23oa+*Lo<ij5!tuR4AW+IOM3%9)#1gp@ijhBuHsOQiwrfPHFxexz^_tG{Is2|H^m+MABrZz4Rlnr"
    "At0CS+~S?4De|DnPalxiCea~Xg2Dw!f9)H^Cd61?a|P*|C<-y4WBlL$1p4PuqMsP*F=B#=**u=wjDlzJ;KrZMRj?ioF|CQ5K0RKo"
    "yguT;ECwTvjL-`j;FT9F(kp_WxHeew2b&ak`p8sVH?#<BQ2_0gweWcq!Cd)etioCNx+<vRfiVbE=-FvUB89ZMR9<Mjwx*2?`4Dpw"
    "99Jq7dkWmmGzG*}JZ_J#cbYzSL3`R!DxhK`&Yu&~E_%%F1tY9OMI+8I&ULTIB&I*;n)L1KNhpQ7*nH;T6z&sR?kS0;ShUg-ewvlF"
    "4~6r&RaEGjjB5&7Fz}OUkd3`y+2d2wuhnaHuNfvS_$jaDif1RbBSpJ3&D7D!7Qu#CEFDSPnEKBOK+H3Ut5qoZ#Cc_~tC3$y{kdSg"
    "!u87PxZ`B(L!nOa7SWm`VlC$8e=V`zueQ5@o3KBVllsSpVvXZ=Oh@WjG)<owfL;mWauoQKz_kN=)>*KzcBLj$R-xdIbE@(ZD%y%H"
    "VdQ;GS1CF+)9?a&DTZjl+o)2rM~igPKx7O8DMv@KT7&)kVBUmjSH?4xp66y#rJI3ft|-5P8xHI5U3#M_)r&I*K6Mgo6N%1U+?p05"
    "ug?{t*tc@Dxc{a&wh%`!m7zTDtb{hQ?*y#piseq&YO}c0dEY!FBA!+}sFLwI*>M~X?pk#=)-+Z?u@*57E^x&9og%9qoSEd;il3QF"
    ")muHq8by_gk)QUc!e=|<tVAN);WGxy0Y$q!@s;LEmE_d&_T*<Juou0^=8hB<cQhA;ia}L^^&axQXz!g-Z&i2=FYg|AMgj`uTBWCB"
    "$fftwXR7n*k|E~vor`L<L9wK@@Jk&xbRgiKmT|DD2(1h;f@1Y9C>81)3{Z-=OpJ<cGZequlauCKIWY{Ia*v2xI%<J5T==A<fvJ$A"
    "MCq1d;T*K}3W*m5Vp{!&8AEtoA65V|zC0_$LvKg3H>cD-M7$VhpVWhbxMBs8(U_{(0!ACu^I9KM;6FrU+7!Lv>5C?f@m#R}8B_Y>"
    "YKS$|6nLA6`E)%w1+h}YN;-0*HwvV*1qQKHTbrX@E-WD&m(zMSf#ffIAGgM{ATn8soeaI{J?i%}q97!$G|2EB;shdWB8P)X^f;3n"
    "_=?w>aVvX~#C)Z6l9sE8XbJvl7to~>%M_dMbUsxbj7?V~9XHs1E8>VQ>c~v}d$c=tM0)jQ@QqE$H@1AFX0%PW^0^+xGsZx};_30R"
    "hs2Dgch`j?1ABy$PQn6JNity!0MvaQ{Blu-_Nk}A&2L0UCfa|AzQ>I3k7$$C32|kh5~i1_oP~kAsoc^c-V5wOW&L$7?v$2?YT9^6"
    "Tg%E8r%buAr{TtlncanK0IeFH>IK$r$5)Gss)Z-@fT-KouDMf1HZuf`ce0JUEd&V2h_wn;b2H@XI8!lt+tXP9>-H%xWlP~>zovk>"
    "6NQT}4?aD?*FxZ{wq{1<LNCk?tOkYSkI3EF%VVLeP@;#TFE)z0_Mm-C_gN}RYC^|gkl3CJ=9@5(3#3m``BbnvEd^15GsLY>LmCAm"
    "qdp^!rloESRksqTUWP~YI98hN-1ACEx%Qt&gc{L9(3K^GpH91|Ldej8ivm^?@zHL%VKyWAi4T$3qFuaqeO(&IOW&_fctM#Dx`|#N"
    "zXRG#W)dhwJzoFcYbm?S;5LW8EJ0Iytl1TP^<5RYk2aRki@?OPJ{SCM*_2-aQmbqGQ3{0tFo&rWFW4_MPoFb$*hbk{@kX(;cbjrE"
    "*m4MqxJ<Nrt@lwHBr6KEbS-A)3oaQ-xDz!w!t65om;O@P(KYd1#B5pyM`cR#%9MhZA-x7t(%;Mt2!fL0Mx<sT0JH-4=S#btuC_jv"
    "AV45XAS5cEEaEv(3~#zd3{plm&rf)Leu2V~+XTHOByDuC#JLqfxMb5&Z|D&jWGt0f?D9AnFDIyWw5@Xmx5n80E4tM`l%cSBltaWj"
    "1v{g%zRhjoNjn53dd*$1baAmk^{7t9(X}|^+Rjs7v;%#;*Mc9+D41ao+*a2Sv&AT{qJ16|b|@$);YUSumrZAG20zS*nec2n^HZtV"
    "1=^TjZQJS2++ZPr9*#Jao(g%aU~!;M_=qclDJ6105fs@K#R5{rRn%NTUO>(@E_8q#_NB3|75`k0#3_$z0llOI>x7VU@a}k9@1TDv"
    "!>2Kk;}}Z6wAOV>3-s0EWHCn*NqDX3uH&{|*qPWEX)yoBO_PF~=S~tYnyQT>%B487EB+F5qaeLIk`?H71Hk>-&qur#5}+rDuo%S@"
    "<nNeq3$BW;;U%>a%><&@f=}8VSa8>Y=Mv3K1qE+pN@-4gE%?bUVti0fXQbX($Nt<_uQw`8+S$O^7~=AYe}l+k?acu1e5O-NcmVGg"
    "ysGST!F-i0?NP5DT>Q`yK(nB7s}S)<o>$pQr+r$1idG|Hz{<aY?qS8jsZ5cD8~XF=^19S9-<LA|w7Vek;E-czo1x;243zdyP!i08"
    "0$aib4BO^GqfrsLpt(p9Uf}YT6YK4As~CQsK63+g*N#-u#aTR1MO4BoV#15D_U<xceoLs^($lk0N}z2^S^SAk>z5KiPUQ$6XPt;e"
    "yKdFnv*=_M<eLZWz`J#ED6vO{CcWw`GKIZE5HQi_7J+>Yyr)0UIGsaF0Ea(E9XWl>s%LVFU53`ZrW0xh^7Y2zO@V!R6hx1rB%59w"
    "ZZq5aBVOTS1uTYODy&qweNP2bh2u>l#-`+4R<0TKux&Qh-nMLp#T`S;Qy*E;!OiCbemvM&45Eo`3R}WZ7_n=g(lhOLMQd`$J$4c&"
    "q>(C7CC<nvHAEVBL7Kc6^Sm;tpG;A36qJ#j*fZ%AYoqFymm%h$ml~+=E5$OSshB_`Z-eNFJb~F}nwZ8HE=^wx)@$IF^O3Yus==&<"
    "d(##Z4eg35-VP)(=^qRN#wIZeUM&um%yMGACW5LI{IXNa#@tb2fxXj;nca*r&|hMrnXxh3H;$9A$CYY{^S7mMn>HpWVGYs4rO2Qk"
    "#1!6Z!7p7T`bjI(7gQ;z9x6fM3)Rpb9b&J`=e@XOR|pqbgib65e|u>PGw`~k%~oCu`acg<>c60UasKU<1ohcElKV*q<m4f%SmOjK"
    "aEwDHTkxEbam3CU21$Fj*T8!=A1f7BgDGICghjCzg=ojm6nUOF_AF3;gmrqwNH}>v7erY{mD1|SS#c?q=M(;QC?i@<hnsMc5xdl;"
    "6s)#QpH8t7*Fj9{v|l&wltDztt&Zck!{kgysnF&65=YU3pR!~uM|c9B#|&{11ulkJ06~t{nb3(VDt=L?GL2D`DxgC{=42kQgL%q)"
    "zR|D2EulHUc;9M)#$+Kjz1TUe>Bx|9SFpNxOc0ObR~qJ^3c$u-J>H@XMnEyO*Mj-Vt;QVAUFopMa%qbbo>4-<;J)=>8T3-d3$%Kn"
    "V9R`+wy6;cjwkSlF_-*27GI=s=<pvOT!X5i{)Vqi9ANP!FoU7U)+Y7VGLDq=l!8;<>*NbMhdzgLA`Uv~wcvHbRDh(0i6DcpoVQ$S"
    "iAyU5FZwI7{8OOUsS^uKR=8JSm@7T5kMyIoO-p)C_vJf#VRH<OS)lOBh^|GKh-Lwi{yGHJ&fJlk-7pJL$2`oV56a{ViI^#Th0WT0"
    "t?2%4XlRf^hp8K7s4D)M8^VjJXO9Oiie8>^%u>XN?V7m7B_HV2Eba($Ab9y&@U{-=Eg_;uYBeGfmNq28f5FZyL_;s~WUM0$BzJle"
    "LKkn;`!)C|vbb1#d>#uMK50n=Y=~@5Z+YssGVLk`Iu!)U)B{8zV~78Br%OMe?20Siu3P^<7TVs>wS6TE8||ObZ>|3r;wc3xxfT4("
    "ERCmGn?5&Q@ES$@aSUxoWxs~MlJlF38hvIf1Q(wR-oDRdkM6_@XiQk1P{e8Q4Azijm3YgMlT38k3xGH0@%+VOP%s(4f-&E_^pma@"
    "7Ao5GSn();mbsOH#iavXv{TQ8C}QF|L1a~5b`3@~#N=!M10(-f_1_6EjRXO;uSlCc67gu(59_^ON<Y0@QBb78k~Ti0@67NwKhYdE"
    "mZ_=G^b(pccsFjppZ=-YnL9rNuYq%K@$6}F7DvX;&=D9CCZo1)<v~~V*cfb!3yR{P(s?omgHrQ(U6P&kfAZrg_@mCP<J<IykG=wz"
    "Ky$m(q$F_Al1Dj6pLf&mzI8T?0-$11MS^zi@wzG1iuw9u_yTI93N4h>SEH}1YwLw!%)-cIWuaq+JWqv>t0?rjXQn|zp{{hn{pUNq"
    "GJ2K#LX~#v(FD^NGwvw$g^Y2ew#77_rX6LW+*Ui{^;8`_`zQIPgNo4ebHQ*2XmUDUm`Ju3S8S#iSKqOb25J)9#1t@5acUvBylXy}"
    "2ZA-Dmx1I*fDXS_{N|8_7?pL7#+;wpBJoWBR%Ui0f94j1gZ4Au<}3b`%lX8_xjx0h9=G#(UlXNTuv`}|{L+7oE=nAh9(D0xTd-t}"
    "%81=lgsxspY3NmoAR}?^lwD9HkEl;5jhWYi;o5#BZUY#Fpe!Uxq@>d+(I{pibzus~I4PX?)%9j|)Dd~?VOp98QAeBTY`qq|+D{^1"
    "i2C57h-EVKlj2d0-l#E(_`KqD+L(Td21YLR6<&r9+ljp@Au3ht=al|D7~l1UHoHLm`H1Os(<;9LqugLN33+p&eC<Rr9WJC1c*e$<"
    "BD-RQ$44!rN9CRieiYaT{nMa*!587FInRKo!RkvNvqmM5EW*74jmg{=F~L6_t<Xxp+H!*AT;k5>ino~-)$?!!EomAQBb`ayUJD|C"
    "rj1yfzNjk+nHDXqc)+mL#y<m*YoupOebu3}`>X(R{8bbYc=UoV*)%AM0%gM)CNGG64yW!c7S15HTOO^s95CZk=wooNJlFf;H+V22"
    "uHjK5-WJCaERjp5owM{@P1Gx(sh;c6fRo$OJ9N%AA)g`K<CFp1^XPw^RQ%c*aMXdEh&aaqm(ScH^fT>!{98}kAXeE8qngn-cO=XW"
    "ozbZAx)RF%-a*kE$2K*7??fk=U`7<w*AEc&>$wk6&am)W%6g-bm)v7z27F<z)fi-tuc0LLgl|K1J>|B>)LLMm%hRub{EXJcK5cJN"
    "CFZb%37Rs8ejhCog$)*n;)7)KT=2UsVNMhQFM8e9)(1sRL7c7C#EgT|uEzsU>?v5%dp?aD3P)D-b#)UUUBf>Y{0fS~4EfmnqD^ZV"
    "o@7@jdRL^@bq+?)$^9Q==e8qEZXD@%(Y3JV`S3Ig9syq%YhZzW{*8~_RXrC5im)tcM$<pLs>sX;W@bb}wJf0<5o=G=Q8n(F798Yh"
    "rcRIjTrmIjot)S<XnL3!ZlTnrxWZ+V@&KblIt4ojPsBJ1@ZSp|X7u#MepfB0B=@!8Th(JauL9kQ=piZJ<<pX&)DSTNaf9=zz%Ja;"
    "@r&Om$HV7Nb;Wfn7}q{cs)W~GwO%GMIEcdDMjytGK6L^ySt!kwZ5LQ;OeEMM<qE6JIF2PIPO5z%N|pBaf={RkIiH^B%=p@*fnB?y"
    "b%SbA72`Z|i5UTSDmCaq`h<vAf_%}~ub(E^e=hh)BZ>DJLXEcKVj^k<k0O7>dJ3c6tstWX?An!Dy7+z-9z8dKxh#cwuWzz1Ug?8|"
    "k(d7IIMdec)d&@3#+<-0Sf&`v75zA6SZ-yNjJANx2x0<aIz2iuy7CF6{Hp~L_!SK@5`qp>h`ZZGU-IUDBI~J?VjCh|{nQk5uO3Tv"
    "E)qv7$FLf;`_FTcU$0N`S#T77f}%#_%!~_bFQs&$?oEl(hK14yK88y71JN}E%PBx19InV4zi^a)gfcRXu17=DRwZz3PJ+{67a>iB"
    "n!Y6!>!~+`1IYIVPU9FoW&tdPpjLZm{_`8YRlRB>8A!BrZT;!uyZV^LcwyjT^zakiyr6f@Rf$0i!3x3wKFElkeo9c#R~H`x2~T7D"
    "F{Ks!&O0B0cpbFP*lH7zLQ$_E$uR?3o8$D+$0Fh=dX>^HcmpwG@HKs*1)tKa$KeY@fQgBawFdE3krXV}0C@Vl;Hi0bDgp(-e%dqj"
    "Z&cUmF)E)5UQBDhq6h9gp~4FSODLo@p|R$%pw%~>qVlaH-jL7=$eoF(i63QZ0?f;2S~ovoRCvbM5gzcsD4qK5IfCo-ej4hv+|tlV"
    "MSo|+h`_YM<Q4ZsLZ@QWR%}rS$Is6N-xTa=Ga&^}j#%sL38Ddo!NPrl*?D1vMa?}YCTWc#O<11FA7hw&T=xa{!$WAi2$?;|G$3n)"
    "$FJa_fKyunXv3j-w#9&na(b~5SH3y*biD|a0AUyY-zI!6_+&&JNn!fR5Jo3V&o(BLdOaav>rz(?aGj8beNx7MvpR9AZYkzZtB<Zg"
    "{-u<`xYRRniLwwK%BYqkGOPf#Vrtlnp#!#;MGI6USw~@YC8U%^C&k=!<fHP7TjJ(l^ddZc?dUBW&nRbs4Fa_ugHJt^Sh#NyH})Bs"
    "RCMcT79+%^h&rOxd!>6QA*UEVzu{}cr3CJ9r=vWJa6daCGy<nEb*lIY`p!k7ppE`P{YnBfb@wW8J{v1fr!(|FoWuY3pL6}6|NFnS"
    "$^QF41d6=<U;p*rfyuI9g+&wIBb}j6{r94mso)VhLW>F}qbS~Z9T&uvv=A2%T<*~5(#pBdm}kV~A8Y)NYr3B`t>LPaQt4>gjzJkO"
    "?xFajw2x2vY691SJ8tO>Mvv$N9aI5F@j`pA>wSCtjW5<rKWi3+D0f=xzJEsh?dSKjD4-n*{ryAQGe0j>a4;M@yv$5)5|qMr+nE)$"
    "KgU93Eh3=h#n;59j2GS)r&y<Hvxcri!F2SAG+%3$-(M7;as$kWLcRhHN_lWT?cEj`M(jG;Ld8)_X{`}X8)qQ&U{ul=GIe;N<?l7~"
    "`w{gV^{5iJNJ*j*|M6uvdE2QpHRu>ZcDM>THpfL$&CI~3cA^hK31O~_HGe;%;1`3Cs7Z)yPXQwxxlB!z>nXaGqWHPaMa$MI>LEH{"
    "1znlwQ6e?yAP6~c_qn81qB=_^#>0<M-S^Mf`0N|5l>@^Y7eDlDwhHfN^f`3n7+K4wl}ty(F@+yZ7qoL@AP^K57V2d$VF22RXzwYN"
    "4?NBHzOlhRzbJOo9RB2JItzMj%m<2bmZCS>yBLK#^XwswrGM>Z#PciT!zjaQn`*z$wl3BT&q4G;3e?z_a$m*AWEE^IvXG3Jxlo5a"
    "Mdx6d(8xi=XDYqmU6UJXdWsSFEq*Wg9!Xm>;^Jt&hE=dk*Ez(2)+tXa$qydPd0I^o1W-g0Cs(M~17~KGQg2wmotf*eHGdy7Sm6o7"
    "H001{Da(r#Q{F#Kr(*viqos2f?-nj60y|}t)$NQ{)T1Y^a+AKz{yvi8>%Y$_lCPIWJAsEW(w@EXAQPX;2=e8;kd;7^Rak>s!0rmP"
    "R&02lv5@2#GDsKiw)}1(<l<0aR*sC?KHV+mS;{8FDCHXvzu=BHX99X7t#5>c>8a}Itu0%+BORIVHLL%9x5)-b<eA12yC`@xCPhxX"
    "@n{-s{L$R+6+EpnQ)vu^3a5yXr^)nz2!OfSL-l#LiY)dt^cSO4)kZ}&V~KNkwB?v;5i99Y;g(*}l(2nVCb!sO62(|rMn}K-o+2Jk"
    "{t5X<2<7vK^zEOxtkjxeD}u>cFbiQyOnw4$8@7smRza1uxavap@r;je(fFRGzEh%c2ZP9Qv1WcYp0hC#@06JJN7*<?e0_NO0>yMW"
    "#^NR}psBz-3$_Q1Oo_WuZElO1`g(D|kcav`)Lf6yjbxb`rj;^u;V*I9!Y4BN-#lGxH7iKzaT!!vT<OtC#3o0)aF5dl@wMi&2@pgN"
    "bU!Mi>Q795hJ~f2QYr073MiuR``9?q&6*Wo#oGA2isz)Q;HS9h5o!Eb)BUcQaPVcW!<6kkM+lWr({Na)Tsd$*5ZErJ(}{xf4ZU-i"
    "Gfw4?x;+xhKhiW`Oa7)=@yjT1&6%dPxK)qYmfm5kZcZWiy~mj1DN4*`pg0({;YF@T_FS|%#S>Gy$i(-W{x?PoggV6$(%*86em&E5"
    "ZapV(G5W7>XP1X}>OVH4^LrJvR;CzoFO1XD0Or4Qy9*D5aSja()Xr(HKK6(ik0EW7DoDg^ZtF+HB^kkP<+-Ek<d@)guIIR0W$JOW"
    "<}Wq~h$$U!g)~4JG?GLmqob)QoKh*!i!0J(YQ?te7NZx22}+>8qa3rUP0C6BV$J7xT80l8?aW6+Y0~7@kZW;TQ+e&U?T+r~1#1<B"
    "qiXu%_QY5u6%FAm{rLTTtQp;t=0iSRyNddXEa*oI{~}pGsQ(BoPV3;0_<|@wDN1k}4a%lVj7Bdt>vPP0vE-9-4gS5Pzm{xQq@$T{"
    "VN9g}y|u&^U7-b&>BVC76%7_3(6@!>dZ*hwS^?MZHPdetQhXMQKVrXuu{*k=&0u^+$yVV)aA~fM4sbyD%ZlAs`g?Rdwg68<&1INv"
    "%taJR@j=1C(`kN_6&~blqpK#+=`e5#t4oN^BW2f-(L>!X)sP*x%cjBW7!<F{&P(S{Tdn|BMu^8+_G`^wRzeHc54HUMnezJ$tst(L"
    "k>IvN%|~IqI4+6L{iKdWCoarXTvhhK`?Kp;+K3%%gX<pyNgjdniz8Vcxb9J0@JHPpoqc={cw)sFE7lva07IFAaeO)yl5(3uMF5w3"
    "OgzLllsa9#IFjY}NQ!m)(^`X+iyIg>3i}DG=y7x^^k+2$qJ?Xs5(Wlx$QZATA<7&$W#K*~#QMEteU?N<p5_|m2D7<2(190VLiv3P"
    "R;Rs*G73e?GXlIyV|FLKa*E()ikN$CTsOX-(wpZ*;+|bjMZr@H%G6+$kiv+L4;_SxYqtrrg{6?EA4HgA2t?uCS5fWSnMug+H6QY&"
    "U}w0BM%Nf?GdPYQed`J^3^(HeuT+<pm{RCS<P?k^7uP8i_oBADNV>ns(fWeFYX%V};cN<gNkAs+iTtcuJm)G5QmsRAaA0m&n58fl"
    "8EirH(+*OR;}(CN_To9+Lu#0gif42$(N>!m!^kM60fL0-+rZLYs4ohQ^S4(}c;CsBJ4MP{qsd}7bFt=g&yWrEg@~=H5Y}-WNJCLq"
    "sn{xCHP@qP%t#HnH5>E=dROK~hmG>JuqhqcGhXbWhcDXf;DM(Z6uD7au$#kXNCCTtQ)Mt>bzCHhYoN-q<|w&{ap>`s=(MjDNcP3M"
    "4G-|<*qn<$J%m9JH*CF8?W{VC@ZHc0)*+e<&%}`;_w^j-)aiP#$qIb!plJCGfc%ra1So>w+zr<E&jl6NNbbjr88HgM+EfI>l)|F`"
    "(@+#dRS5q@gc-$@#uj*t^Yq5jft?IKz}lnVYd#szQo<E+P{!M#I+T$_a1y>#V|y{pp;RhTXPd|d-PdL_dD+7tvg3A$skZV;`dm{9"
    "{f}ho&!Q<Ib;LgR!myAr)`sV~vS|o5aVHe0pQo^Bj@8!KJ-2RbGlePnWiHlyc$7s%`&5`21|Kw%C2p)68W7ef_->tHWw?;Uw3vF_"
    "RVQGgw1s9(F{dy*zTa(lIE~PI6*h@S*JRTD2eM{R16oZzX>|%ewi|BQ&ZMU{BwC~9G%I*il(N3=_nHsCu-vK}dn$%jfoQo&eB^=z"
    "35tGBpXXd@cO{Q!?Wr^&8@I)#F=X%TxP7taZyfO%<N0`Q^2M2=EkaJT5cUP;Q#gsD?xS=x<CqL=tsz#ycQhT7u5{*<+c#<w%6ZNT"
    "U6J<0Xbf<mY*>W4=6ua0T0gp=xz$!L`)F(1Eb1hzE!3tRrf||>0^e~YJir@oOMf~|0HPGM^<WJ5>ZmB+j6CY%{)YC;Im_@EV$~sL"
    "tc971)<4|!tJaIN^2Dyd0Gfid_RydMn>Hlt1pH6KNoo5SXbJ9%uyd?+w(PdM?dE}4mHQ%x0PDq?@#V$878|I>ZJZYjH(kU4(X!RK"
    "l|8EW1?P@wv1A|BkfIb`nWta$+S-dPlz#aIP=TI79uw<S#p$#Tphho}7AV0N+w}E*1YNXKebFWZJ7+vAL<#h<{k-^`=@&%vE%izf"
    ";EV`hAIB9TsEQjl^LI{cI3kfQo`Y!H7*#vn3N<>$c$NOgz}5O945cR(aoH6)D$2CJ9T_#+FFo42!flhOXj@7wQxV@c3SzL;W-`tO"
    "&HH6m61w|NMIryLDf}JjOjbzHpG`HiQ-wq7XK902MC0SoFHq`KhCx<>TVX*xu7`tgv*Lul&CT&F{ssOOqa&KlyTU<;a|w-VXQ3`M"
    "(Z~w=WA!f1d}$)<hz|$UDGO~sO+Z#B<z^2(AY9ML{DHowM*wU)l#TdAF0O{IR1_LlAX;d=G*s;L)<yV~O_kLg0!7+*_#Aw%Ss$o~"
    "w_?WlH&Lqhxgb0^kj+Y{S@<p}$(3e9;2$1~F`q@wLgYZdOExXfo$_~xkM_CeKt#^|lxhS(&1|*_ZX-P$(UcHES_+E#Gb7Wj3`z_{"
    "@&fY4OSoa0=IM+5e$M#9Q%c>AHVt8gEr6`>rud4&_h1wZj>)!ENKhnhT%FP32C@*g5<K0N((Cf+V$EOZ&)|G6_NG=7pIRh!R!`&F"
    ")Q7e78w(&iqX!p}t+cBPL<Zs6q<`VFg{WskRiuf~=){4%^|KeB!~2J!D2x~)bknmF2kF&F%64H-q&|=Y7hgfL#v+xtPm8Bs2@zc<"
    "%p_bn!$lb@hUa)OQBjUWDt+Rk4q@e1QLGPK$Q%bE&uBo6$`HE~J~Fz1h|p>`D9CySXw1c$zu{8s8OH$h8IJ%hQo31LrlXv17>x6x"
    "JuVd=OV@jKbbKgtEbR(Rf3<~n6*t=#TOj^!0WBvQ#O(G)Xa{9LJRU|9QWp*lJkM!TRs5=YvkGOA0ke&=0y@<SYt$yay;$=((#$*9"
    "di?KPjc6y>9KFbAb;2)$anyktPggn+^X`MAB+#T7sSRflJ7^;-fPb&~l(df5HIJ#JN~rgnT;5jL4Mlj*{CQl8z=K-!AQ5Lnt8B--"
    "wHdRt$8m$dSn?r%nCKlFkJ#yk*j)1_rSQHm0}`!|G>#X=6B>BLJ_M6{!T!)G=ByNHZ4ijM*g|jii%3IJ*rp3k@6{STBeStnn8l3s"
    "W8;b$2I^PZ*@CY-F|RnAX;z;$8olHCeWiyE-tU?LGenG$rc>0?@mBoW__6gg#uSJ43^EJ-?CxM#$mO(S$AsL05>YwBiQge1+2?ph"
    "p^}qvG1bekWviqkrrBuEYqYkcl)1CiNwIq?A=WTfA=@^?g^4mrSZiObncwee6k;IrnORwZn^O=$mJQ$(nxLn36V($(zFkh_qVLI&"
    "wWz$PeJ2B6?4k5z1-xK^yJ&!{+yTAH+1%mLW7mWxL?jq9cbFv>JH@;8r7JI7;g23A{R3W~{9?`bN>Q6OBdn5`fhaSifs>8suUB@<"
    "*o2fx%i<ExSrO4)R00xxwWuP*Ge2$e#hOpI35M*bmT66qF{N#RQlmGDmOg6j8`o6&=9GI6z3hz5?nv7WJ}yl4Vvk+EFQL*K`fx4H"
    "r~ow{sUyNs(7)Dc(D$P;A_-(C3@VCFjVD6spF{1~Ojj^j*J*MW2r4~QZhH)IPaJ9{Y#gMiE8?kbv`@I|@))u!=qPt)gUO%Ym`2gF"
    "&**R{N@up$i#79;R%o7~z}Zn1n2m8DZCfOgg~^L(6~kszFGh)7h=vxog!u-A1*Iov+*aPR-yuFdm34fez4_6uLDi^bJ>aq^@L*5T"
    "y4G|$wMynFW;XGVS@eC~*vy3Dj+^v`+eI8o<-N!>==fo~F~e!>8p3U2ZaSNKYKQ*M@^;XJrL|F44p?$4*(udgbv_OFxqdfiAX@i("
    "ky#NHJigdH*2zT;t%1VKSbm@fD#T2;Rx3+qbT~Ah4}d)?E8OZ10lkY$4^-WL);y+A{!D#h8bJF}i`aK$Bti9tsn6)eD*5PVwaFDR"
    "^tN|1I7$~tmR|FZ2HD4&PZdrfMko3x)+%)sLAl{=?JI7jy9ydZ0kKu(Fuc!&C>AOmgIh%(D8LF?IrB69^|j`c%SeA7%|kS6UBVzc"
    "VJD0~{nP>jCtJj_P4^+6S~0V)88Leumbk2?@Ke9fXMU=1IURwCn^IAJ;f&QgHWUhsvAE?@h%=0Yquql}gf?7bzE{M)%!C7z-V5y)"
    "D6l(>y6jY*bQfzr?<OZSlBqZkaizvRJyN<#F|;2GUl*p+>2+#&5tt*SCh)9uE3(hxV&(|Yf3KNd7+!Il#-?D-JW{6g>V=mmk&c2J"
    "jUCL5D>b@fhAsr(fgwt`6Dy%Xr6$FTefM_D6nt_JyB3V#)iH&*Psgta7o*77gcLC|^vvLCn9rre+nBW}rWr;8%*CrMFPy1Z*wHVK"
    "6X>+Y2_h<FD!UAPcIj`xl|8#N|6Y+D*xb5z+(Hz;M$>)_dt9vflH3q}bWI%A>rAQCNqj(|A=2(-6(0rt#YIr6qSI8n9m_l111JV9"
    ";r1H7n-<DTa;px<@nGvkyQK^6de=8veCe25^x*YB;3pLsugnG^9xU(#1MpT(V`>-eYA@D&LKlv}1%39-<(f{lB7#D6N!2jhnW?wy"
    "4;DUT3^<A6Z#1wGIH7u&Luz8%qP`bv{vMS=Qn2BM<dz3^p%z=tPN$hbq}C>7DXtx}2yXWUkuW;yg{<Ja+aX3644pem+Y@bTTl6U}"
    "mVB;TeN@KGN9d91VG4bE@%pY2R-!*WPnx&|#ONPU(AP<(^eN8Zf^EABpDj8bzt?<wNTT+6K_FDn3;muzb&sBRVP)>qwVxIAYaf#l"
    "G!bdq!#7p=HaoXT0-TL5t&25(x1B58Gg4U3vvIHO@R8q&MZzCCO^&`J`sxRA(`jPh+>_aQog#`wpFSd2A%K6eW`0SoupVv6`X<Uy"
    "ePTHxREyGjLds$t8w=6OJh&}0rxHd5kZZemROq%a(>LlW&wcCB=*pDgyzyDgqb|$_-L!#<MARA9V>X?!RkzPdT-rEgOq90zj<g}6"
    "x!x}t2+G^fC;+LXX=&(K5u19i04X>?t19x3;_!m2GdhT<o^x#N^QCBR#P*908t$2bfcaw0{MSTcwRLG+5v>&=P{eFFG{!Uft!FPi"
    "QtS+Lq=T`wO~nb3G;@viLdFtOy!f8wIi4A<(a5BB$mkC0|HUE|GMjItRuix}MK)3IQb$bT(gOssd{QbLM*>5*{^B`bmMi!lPMLr)"
    "8jFO6K3Ans7&;dO9l%NnIVtUle{CeVvJP?KUa#qcCmPbm_nP%-2SBJ#sllT*tK=&JUlnhCiOB8XkWt+tpJu^5og4s#wp@u&2|*FX"
    "QLza`{=Tz(eHkY;>*5Zgr3Q-Oa&29kL)@O0VvtqaVbVE>*UZwR+15MP2TE8A0qQhXzghD+pAiifX5xI^c@M>*X+`B{UAP?j<FFMh"
    "_nQH+w7e~v&0U#kH3lq&uSDf@`;J5Pi3tM!N@$!F>Y%t}_8=C$j~z&=;y*N2tf}m8mCcPuMM?Bs?3#1CtmocDd+9qS)aOz$K>v=S"
    "ryZG&Dz_HXfU8cYudY?+7;tPcKe2X*ZDUL{k$Mo`R({FB{`jGMtr^}v!3fg8YE|!QGpX%qb<3#fQQ;Z1(*#(2S^>Z5O<Qu>_@p2^"
    "r0~GjzF0Fpvm+c8b3%L`)eZyAX?W!6rRj_alJ0)4)$v~p16ND&goz~9w0bFR`xBtPmwdu)AS8s-%Z5u_or6wh6PJRFVDpNk6+KGD"
    "_N8_@cAK6U5B9MVf?~_9EIhO?)_k^57V<y3m&y;wSp&z4WBNyo1$R8UMFjD&M{;?Kiy)u|t4$@K1JqJfV7%yMtM&IeAwe!AN@`=%"
    "qmR<YR`3(CY{G8JC&JsaGm^^?e8hx(IK2frZ;v*-r5@ZrKc__@?sp5(?M3=*-2UT1;866AUFU8KC@?G<k>~KxUgVSGEX?h7hI9po"
    "76jPleX-^PhwvCY?u14^>T`@+XF?=krHr;2;}mwCiSicFM%c%EM4!<zpt!w>g5_}S?={~s6iWB!cqRdYpl8L{X=ShIR8V=!9q%g<"
    "8!_LFqfh9A@Pl9JK)i}+aGl_KvF5Xf5Wqd9;7yGgGXb3wY<0*Vx$A%;d}3S343ZMhO<sZp0ns)Wn9AOYz~97Ld)2)Cd&;;>Tum5y"
    "pvF|Nm1zSV1uo)7p#@fXGm2+1X1@C-$0;ChB^K`F!$o<^zT;#{Q<Tcf(H6#1F^FzpJSgV66(%s46yys!#|j=zkDioNEL6Ttv#?~f"
    "G@aG(z2<Z0Z<*0Km;PsF9R0_nL?K9`1QqmGWuPuD=%9njB#-toyCI6#DH40Aub5D27tfjPo64%vGBq5G`B0)@3_?5h2)8KKq1r@6"
    "K_nT07ZK;F2<@G=xDhI)feQcfcCqGrG3^4ak~%An0&JZ2=v9s2hBz7`uOlt04StJdES;Z3H@p}*Kw2RXs}_XwzSk@-bwJyc=M)3M"
    "Sc4lUp#jBJeNCfXo8ZY5&9ujySf1-n7{yUq32bgeq?B<l&S(97w|tFi(KOG|VPr>pd5K3yanV>Kz*(5<RJdKC@t^_801=g{2!%-a"
    "!td8zoR#{IPDOiakD<{--@3S@nMd)jF!Ln%9tPcB1%;}RsQxPX4gEbKHqp4LUlt3{&oveNe$!_tmG-!A#;TurAXLrFf6-FX6!@mz"
    "k0?cn!cerW2|-lUm#4CAt4ygWrgw*HrOqFfDl7*KFY<d$Yu?G3@Z+{-PML)}Wu-(V(xK$zYSD{Pbg|Pbnu;-os^7rt6QSTJGK8a)"
    "073p92Vg+(_k!-+&fqH*T#L#_h=z($W~L;Wm{yLUNS_7FLJ>}EH7X$M^ea=F)+%=BDBXT9>D=2iSj?&WP^D0KtQZp<hlRszvQquR"
    "EkzImJ3y>_lo{<D_`)ddp`6A<{QO??DS0PHrw5;twkKiTVui5vf${1_)0&o%6nmYvFm2-2@nh_Y1j>!bvsy%YoV(a|4?4vi&K}82"
    "q}avIqTnt%vs<Z_5IlDm>fab7INQ;`Ye(e3PI`wTy>5=?2z#;SJH&bFvOo)?KWZiArAf0zSz1dWX(<+p>@0d}+o)m}D2^%&95wBq"
    "5ez|&lD^lB&vPo!>Np^sHt?oDw};p$XJZ)uT6DgRhzukXzA@@($H`|vB`E3C>#ShcaPgc^<0+l}M`YIlxRYSgqU%_oRS=Z((Xyon"
    "8Nc9x&708TYKY2sYX@4e%^LlZ-&w&vQxB*Gh&CKzF7zZsk=eJ$s2ee4A<wGJVRjjFw@HghNgZ!;<kVMQl;Cx`zY#H%@!7UCfija6"
    "{M0#%I-12#A!{tP^yMZ_EO-obMQ;@B5Z|XFVTbQt={V@iZ*y@{@>2;e%yz`%q8X_HbSl402k3<gj8ASigrQ77iO=9|$1H(Ix;QI7"
    "4>OJ(<#!^o?{#gaiIX+arAsqKEAGQor$ACc6}=ucw}NbJ6=l|oMEJ7f?IAu!hu!S@Hs8R)KKBnaYlTe3n>%l#7#5&7NwL%yq_%p%"
    "uyotOV3Hzx!={uj-lY(X(?QQ-b^5TMYr3b6n4VL`A(~)x6omMbo&fn2Zh0J6aTPr-=Bq{1nLkq?G|f2-MpDHXULOQK^zSv_YiSfG"
    "k9DsAHa+5W=Jm}qw~310`agX_vB!oIH1lqvr(E70$;PJhytv^>%U-M*-a8L8D!KBXWiYWO5bu<+6wlp?f-pt-zSE?qtcuoJz!(<S"
    "$=(G2DJvQj<i7!wdq>HSIUqvcI)hfPZ4Q)LGn%*osmj)pt%RiM^*R@mlY8P@WN;X>N@2K9hYJwyp1S}YII6iv1T4-iQYxah4EHL1"
    "I8gw`)dy|rWv9gu%|esYWQX{;!iM5sOk!E%V$Fxhm53ubYYOr<CR~_DD0B|vX`=-N#8WFs?bilejtV{^TtlB!IjkwO(`)%1;T5-^"
    "@9Cm8CwEv0gB=kyW{$8}MutS0vtfEeR?QT4mYt02bfM=KA_xWLwdVW9_k1dcBHG%u=FB?_-YAJU<eHKQfc~pDE$aR8EAm`a>bB3!"
    "C{CvHk8Z$WIB9+(6ZhBa8flZG3nmZSbqw0+jT0JkLmLVW^_Ee}Vtih}1s(iCjhtW6<vve+?dm!6-<o1_RAoh|CZZfpf4niu)p;69"
    "g!FJxDeC1659&xNCax(m8||byb5gIc7ti^|TPI>4j*;-^552QgC|bio#Bv2skmgjnXHqs`@Rk_z?^8hkY=6&CrFfdQy+`WGVHze<"
    "T|v(mYd*uORq-lfo~7@4RIrxb)>a+}qcEv9b-eUnlzg-uip!_nP4Ub(&V^MtsR~{B`{s+@!)LCE8C)7<x2avbD52$pMA8{iL-tW<"
    "aA668`zn^1+GxfmCsN(f$D`6SdK(vOmS^@xnC*<?egBM2Z#h3RC;jh#aU)IHYNm?Sjot=+Ga`pf3kjQvs5|=nQl;6k52LRUapc$}"
    "vx{3KZ{OAzJ?1G{&xr+&=$S{b){wJIK^#Ls8ZR}4QYF_)uMHi)&dfChqji`>q3!qBp!Mz*A#jY<(zbv%IaW~)@kk*CF(5{LB9b**"
    "=)IvC*o()GIxfqI?N(5?XK2%XA4>0E+=T@dIxjJ$HBgXnk-prJ>D~7!pR0h3?X=!;r8PBZ5sJ&5Qih0IUZRf|OFqX-hdAeWX5K|}"
    "M?^az9Hsj0gsr&ZGIa|Haka2@E=nEMAZjh-494poE_QLb;=_A!#l2&?(HFAsJe-TnJ<zl+?1fWgY!+7}aVb5#=hH_a`-u%hWmsGR"
    "N@zLeh92bP=%NC+FK+dGc#B0g#)v4aT^apJL$6^2&e_d$8P0i(8qPr{SxAZi1A()m@1}2qj*~)x>EC$&oo(|aWn^y{k)@)rL`^-n"
    "Y&u8X?^XoKu$sxU%v`W2&eXVf%6W7FH5ySX1MU5L%?Etzl|_ak>6KXAHIKbhV`RoA1Ze~M-jUjB7l)>1r3~*nbx!4ZA@8nuBtqFQ"
    ")_jY9$Qc@_aW1`=RAy-&6BGt18)6<YqLnm5DPG^Q3!)ai=c3)(`kd=f9dGx=#(M`JVq(^iM7v!pA-AX(tSoM~sM9J!h9!#!Ed9?4"
    "s&x|B8wou!D8()9CjOdqvF0-kE>>TTR`d7Iyww4HkDo^Q6=<zsTzKjX5^D8n_Ml8R5uTvbG%1;xg1EYH9oC|ZC~anJFP03?Tv28E"
    "*qs|0VdOw_O7D1l&92iFaKll;MHbah!Gz|-gaqXYh3vwG!6On*pKFHaf=s}G_k=zX7a$zcVzM$_SN@(6&1!2_MBPVkkj^K&THjr)"
    "8a)LCs>S7_qVvU??<nt4g`u90ggk1b+7#iSWFuf6bD+}^gUT~!J4CYG@qN<oCBA@lmi}(O;F;m6i|Th5o#)m---M*Ke$=EGJQPBX"
    "yP7HZhC4u~?F?@+H(NAp#3qa{MzM!B;P;YGSoi4DM^v@;VxmaYGH1Fl=sGTX4$vO6N|qMyxpP$L5cV8h$51(1VQcHv*ZG~j!W-5D"
    "gG(R-T&Ks-f2UbGO|vN=?XAB{x2<o4N9rJqA5n~UC<H5%N7#zJxYrF2{X%Qu2=E;BVQpdk-t=xZOT!O_0hsO~jKl;Br8Q><a=i?X"
    "#ZtMHIwk@aOTOwUA~nY$vmV)YP_fXZNbM@s@~au0lFW=hCWry0i=m&`ZQ<NWAA6-5^F3&f+S7BVSBszl3_fD)=ET!Sv`PcbDtd{w"
    "(+__H-wh(DK`FXzboYnS>{b?YT=(s9v1=b0i-a2iSF}AXJkl9y#`Bcqa<ou|SevlNp8B3$53T_1mJ5PS=p=hT!c3gg?<Jp{Ks&Eu"
    "00BYyJ)>hJoWZRyviS`$aly(WP7+P82@y3{yV3E)k=TZA3yOUDEEj9q_j3k@dnx8?KNzkH;+R1vaG|j>wUe<$5clJTnr5am6}QC="
    "M@%~g-?9@fO&4p1XJE_O)3IJgo-^w2PxS1B@0DVbGAsA(C>XZMuwUH;q1r~)ALvB}QDBTBE_Tq9hAb|Ku^#1(rEi0nMw`hi#VMIq"
    "ycLC=m5=w4?8WBE=n)GJ+9<Y?cT<KaUaa|y<Z|VnDgFv7_dL6>slg_|U?RDgZ}eQD>7Sx4&aSTG-1T?43|zS?Ip9^K<v-WdzW7a@"
    "f?$?QVR~nkq#8b}IO(iTc}^q?WzCW(KeusIV1;@QT1S>H&C*+FwcRZFN*f_KNqi4FDoocJEF{Gjogm}Z?&`qx9WwfMddU!c>-06a"
    "Y+MDzhq1>6bBk{pqY`AfUW2#m(`?$8=yy~)UMma2A`K%h9}%>{Zb$e*q<-TtB-oT(*LT(K`d;&$(pS3omIi6I<Yu&t()tn`n89lg"
    "g~+Aph)xGbctx*8i4@Zdu-)n_n=|<KJ@lY8UyU04<WFlOC*BE<puHZ-uGM3_q`cX-F!>UZ##^W|BOCQ)G1XbF|CopDa`A5Mdo`lX"
    "UlLJ>dG%>=KJdtT3fL>~E6z7p?GQT@IPS_ZEih4+hgSkETmdvZQ`W_r?p2Z#&#EvYxSkV-Ncga!Nfaj@0Y{@L3jVmNH-gV>;I><_"
    "Hz}|i+!FA#ya@2|spVILKo&JPAxNjJtU$Y}*jzfQ`&A(c%Zs=iQ<!@SJq#2^2JO59LF0xk%y+*#u6-{o!989O!6V>In+X&6aab!%"
    "GF#6Gj{AipS`h&bo{9?%;>--<k2Ce61#U04&|44FW)G)pbQ4fJ8kg4Km(>h9-h+9OB=~BI!XPz!!P~gES4_6TPA(zTEi7@f=2Itf"
    "0#xwE?ObuV1F<)$1i2bc%~{`ejV@%XT^wlnYb_&F!ac^&+b;=Cu!|iO-_a|qe2!3~=uFRh0XVq+V)%+fDax?;s2PUOO*HqJH6LX@"
    "27@E<c3oV4$EP-R2CvE)YpY7mdUr-R1%r}o#RNc+S!_-OuW1)Y!2|I~z0V#M4iSr1FkVPTU+miSPS+X%JCX{vOe<is5s=zN@&-L8"
    "g*Cedh#Hx~QBgobMM}sih>(|+@%RyExmfdUJmx;|l&cki(?D>gVuPetSOj#%OR5)NSxh(s*@xE~#Y#LRkFFd}rg_bas{A{4>O796"
    "+<X3%jtxd>UH?}Sv>2(5NJj?x71fEd<h{YOMlpfwp@myo;N5?(`E<PqabSZG5C)ue<3p%}V<tW9upWft^ySce8aP2K{?JUdkCsLf"
    "E-N0BZ9&}cHOo_$5B6BO>Nz`dB&UWquuRSNnDFSN_^E`Ar-wdr%0hixuXJI2gff^;uph$W-)q)aq|f-D!r#TuJO#=MY^SW=#Gnvl"
    "nQ<{fsMmlJjpBvkXg@MvOF(o7!buhVFq7V-eY7hD?!LoFdbD_y1AO6bj$JDdybYs_d*;wLa*={7L8!IkLtJ#ke8c?AIVA~gmsu!#"
    "0k^`%lHn2AV|I!YtpGwXP4t1W?cDdm>$B$`^z>*OI8=nu#K3PHII8$f6Eofk7o~{w=6GQ?<)Kq@vCLN+Y7-MWEX0)c9+h}a8<Q*G"
    "CA6fp58D_h&SS4W5)HlaVyCB9(Mw@b8AxAK07Z~j{9*Klw0m$vlpr^;D`9b-S^%qn&$`p$^jmMbq9VMuxKQ}MVo7f^NMOpb&{C${"
    "qcGCL>~>G&(i(9}h)}-)?faw(S5bx$oYZbh-9{wcHYP0Jw;k!F<VN7kvV+k4(0R#qxd}gBQ9(MPOo2$VMW@_zuYnh;ty&*dT5Ue@"
    "q@fITv4h@A(~8n@I>Jgq5>+Wml$YE1h7!SabSoP)T!uMCO~b90NylbGsZm6`f|B!huuM<eu(ps(o|ZE4iMYK|XIF3*vG<PpBOkbX"
    "JqzmTLK1?=+CU000nLBmy`Rgyo-RITN`I|$Lj6d6Vx17ppzEax_wz@sAzIOk_K^0CsGT0QCA#Gn-xJ9)1Gy3~SvSx5RAH50&Cku?"
    "#PuOhYUBc8=laiYBvg<g43zLkIW2CT_9=ypj_A?0BJ#~%+;^n6EzuwWeN_Iwf9PSjShSxb?V1M*2+?T|3(;ohh2yo~dPF==+_@9a"
    "&9V-Q%xW86xaruS<drYJ$8))>uVH^`L;>}z*coalNM}YTL}wMDkU;l4o~zubcv)x?rIkl1Bw<G2adZ7%vOdSFDp<x-JfYKug0#L+"
    "L@K!2jf2D3%9zb@si@<17-um{DNf(NL4G;^^Md)M`Wr2&i2H(`*9u%Fp;#)GiTO^~t3|X_G+NkMAPeUg+ms>5K!PiV3!I5rdp8%&"
    "9Qh&Df^))(Tv%kMXG{7<U3gPjEn#K4khm3&tbTdiSlZH4Axci+0x76@WZP)YzgV;WX{FIT4i@1OQPj@hv2?lsgFPCABe@}}4Q0^k"
    "bY8`bq&ri&bpDMx^Tma9wtril2z+j8JCe%@69j=qpYf2>*_nK3Wmdby$Ba{C^s}R{APU$=677eat?zJ|-GjN3D8ljPia46kL?HCW"
    "UA$NLtXNm-bTHo_=rcpuld|7B_sH7|i+TikD8pT>`Cu-kd$b`@#Zf9lXs+}PDF~xikEbziCp3aGEyz5hm>`(Pa38IV<+1W8-y&kn"
    "(>@)arHJX=M3MZMv``PFxWHw-z_epRNJt>+4LPpov<tfcFhgK1a_|r_-ZV^TQvghH|4@{_C=ldFFOL$@5yfQ|TW2i_nrs|@3d$I4"
    "vaLhx`3&P+hz!wV0Y1@xrf&OC+D!O+eJ}Z3Vq?o#c{6zRa<kfjTeCt>6*rbPrFbgSsKsF&va_Z1uRSXMBvjN_9r}xJkp<dho<w5N"
    "aG<M#vV0N!&5T?KP9c4jb$F(>yP|N?Ia}FuM%(o!woQG~YCcbc(M3i5Z@?bOBTsA1;tEP}D{Aj5OAZTn+KCce6m-nmTm!vKg{~F^"
    "F`=nDBsUQnE%6)Y<)_W1fG0=_416i}>nVE_L0WVL(b=q^B77qRHpYtjCl$CV`Ud*aMgO_cx>1O|_?qt}wg|R6jRPW_iHaDS$wh&f"
    "4J7JXxj%`3*-TAl+L7ti6XMldRsdXkFARq+KIcoKxcPHDCuW61tnB9^jy5~bkTb%hQTZn^_Y;wGuQ&)QT%945;7hN)iMD*MS<Fk^"
    "y#4|bHOfv3yt@ue_?UAS2NX}&2McK`UWZB3{c1Ij+1<IPTnaX2p(!@U#hTBQYT{2rEsx=heCq8sFt60mc2A5Ki-1;oCVd{%Mi57E"
    "dY^Em+34dj2#9{K>0Tn947XP3kswuT=>G_UENDF;ltihx588o)@T9@gGzryMJlefpY@AA#%7EX=uRMXz)6@|1j}jr~opIHsCmECL"
    "^{y|BnbJU_SJcm>hJy>2kphCT4AIcu>6G~%ut&Z3E#DP{v1vf^9m&wumQ%{1*t`75n8k*k!O+9@g=3_l3**P+E#$a#Y@5AE3dKKy"
    "s1g4g{O}eC(1a<(qR)i}swl1Cbi<=XPl&IWXlF!ObfV4HY;Q3u8dYK7_6^#~D>zK+vM~R@|5cGO57^Al!*y_R;DS8nC7vyEFK8VW"
    "!f^D#1?^y@{flxTGl3TkOeMa~NFp^yguK)3-n(*3#I2awUQ7R&H54m^GyJq%^`H@`*r-I7Fsw{88q%onz2>9yak^|OU83IM%0d^Y"
    "?=jUrum!;5u#MwgK1;0PX$$GY9*}R)+t5}*5Tsp9=xfafoJN$lafqM&w0xqDSsQk4-cCe6a)_c_(_u$~^t*m7I6+<dmVR#TEbB}c"
    "OXkNFAkl}OkNT6x_;4t^iD(OTm&^bsT{>NG3iaM)jq;ed((IyrQ~yg!4CrHs{Jms(hc~?r3pdKOqvNC>cSs@YsSNaj;^$OS9rVO4"
    "hZH#eZi)lk)zglvtfItvF=0{NI|h&y2Y{xEp&MbZjJ{OJ;Dta94GgaM$W=(IK>RsrTk%{P>Kogh7$zF!V$COc9?uY=M0&Et8k*Oa"
    "j5e)?VwMh_6Q$6`5%xT;KweuYYhgW}cY7BhcqM?9z7I<E@oh^LMI-Pb@vZo!=ren+O^=mwn<$njonqUWl|ClTk0OqJ=N-ikXi>#{"
    "M?m6Y&3FD+csm)?iqSfc0Br_x#n2E})oB$ZpkX5R!DZM)oX;Sw!p#NafkRQEyWB3;{C%~0eZsc!rUR}%;!X5$Gk>XInY5dm$uk<7"
    "ll$iG2-Rk~)x?4}-7^jG&e)Wc%Tm@C@26l$FV%Ojd}pJTS!+gYUu*k@IzimnR#(M8lud&V)Pu%A%*`f8TUzDRIjhh`Z!#(><6D4S"
    "RQhRM(d!kqz({Z>YXnSiJC1sQnGqmk#-_kZ=viA@pB@0k*UpG1g%hqrcA4qob3B#mjK=*>?`<DU@-(O5uaG<x?Q!#VI=Er*3#FqO"
    "s3^8UkwbqCq}dp@kMES=%+vgY(6Anh`(*6hl-WoMi$;<l^rW{Id(x4`EayxSFO2dOPr(_vjWxSP{V|t|HSH@Qmy}xSk<XlZV-Cv("
    "<jrYQ5v8n@<AyL*XC7~K1(l3eUHS3d)?;d`D<k;tHQ(vNfXawDl7JOQ?Z|8$^32-UKXNb_Ir%Ltfx))cwn3TQBUpnv6#9B%Lf>Ku"
    "xN-eSt3?PE>Lo%ZN98bB4sTt)Am`P%hw2Vl;Wh?3t=M*%-K){67HKc{E&YV^+|x$Fh$IHoLZZVXFcfmBC_jx<^1|F&uQFxSz-`m%"
    "0j9=s1RC^Ruv&45e!A~9pUP%Dccn;~G@U;*DUWA%Mj#;w3s;>`h2hbnt$JeNb73`_IP@m;Z~A?(0W>LJEcpO79M%tznRuEYn2Ai9"
    "{uB3s+Gx}1%7Kwd>Bc<*zMuMlJD_(gB%|-W%KUcuM2q-04P}K&9@C0TFs7u}Hq6A*dzD)CroC`*ZHRviWDFwzC!kuAb~vrZSmCaE"
    "@ip_`#a_mLoe4NSCF(D;QMjcWrZPuq8-vmor)wlPW-<*!DP}Y%lF!+)j#IdJ%=~9Cf#w0ND;~jIw-cg5RByG5ac)+On@y+z@pVXz"
    "@{W$ostheNmCFo3Y3t%}=2ulD;@kqEy7-~jRtr&Wt6y|rq>{ThY$|~qYzB994Fku9MN!RqLqNbKaJE3(+L6}Op%5}}qUGC+=5meZ"
    "H2UEKY>vW6@?jK%fZ&W_TkQtK{c$Z7N!$uGG?0mt=%nXHB||jj;&}a4Cd040jc~e7vO}aXfHhEIsv?pW@m*0tbfvfOx(@ivv>D-r"
    ";;ha#BB0|F&ANAHT#1Dq{^M2@3~9;j;1f=7q9TI<PejQW0av{C3g<;jpzXS<!>+?BgB|PJi!~oFFB|z?>GWH^ErX6kLTbA(Y=c}`"
    "p6C9@qiT?u&`(i3w|Unp{?ie%ug~%QZtV+agV$fs&9C^76?ri@7ibpASp_wAdNlm$*^ezGRI$bN^f5+S0s?+4{WGbwn>GDQhy<U0"
    "nomT^MfQ~AjGm~V7G@DNaNXGvnxPeAZn}qh64uqf3vi5qb20OK7w`7f%b<Mceq?)!0CE~<*2+Z2#3CIi3tB7k%=CgU%tJ2*!Yi8L"
    "ibA;lh%k+LvF3AD1fb|M)A>|Xgj=qpw~8qwmJDLf0bAF6N}CKfM{eM%NbhnMO{%c)7A)&x&9~54QOhDPD3KZzh7}6Za%egRJu+s-"
    "Ot2K;D0`AlVa%fGed_NdeOuwIvi3!gwtq5A_GpA5o+-p+=t520F&d>{r2Y32@!2T!4(*qOAp~x}Btbnp6HCM4yK?cI@zs;hm?9>6"
    "!vs(e#^>Hi(V>!y8&4*vx?|gg4K1kQyU14+kU1qUXHEm>{6=lBjjyPmPYLR0n4e!X;rF`m*c0A|M>80>6l(>uXprzrZ>A=!D5;-2"
    "<^#J{P8VxFvCoN=AJnF>(-`k+8Aq+y<BV1L4dFy1O0eBTII)*rkmq-a$wV|$3{F)pT%476(mkS<PhJrwrAey`4DQ}-%!7%(Dq6Cb"
    "58(DjVA&z*XI5SoAtq6>l$n!J^!i@&O}Y__E_@K8{#+ex%VoTsJ0lv@TUPcEK1A$d9`AN-HN*@|MJ#_9CFO)$<hKpH)_gRsFy66;"
    "REfYBcE9tJ4dgL$RT5ceNVw}4Q|?TW>CiIp+ueIGf2T8(x4+0`uXQkuN%V|fh>_@r$}<M((F;adoBlBUii)2)v&gs!T8aHtlk60h"
    "o?^)*6fc6Tf4((Rvc;jwkAN}V=bG-NMGEvWA6j%Q;R`Kk`t<Hh6qjXb6;@yo@7e{6zKuj~^R0;VdA+6O8dq4~in#92#<yri!)&wm"
    "(V}&q(OxWHjRK~q#OxI<O_Wwd{JE8I+V^f_7;|nN8bXbN#ouebmky|w8|H1&qJ1BmXjk`E<R}O>Fo-{dCXh`cf+*?7lJ#yA(Omdl"
    "TW=w5e({{|ji8>)J9HiBI~1O&U4H}(vowN6hpG&KMo5o;B^>BzWn*rn3G?Xj^#NS@E~mM7w%G+CH**p_GLCvWf%Q@74iI{?Nn`=G"
    "%h34f!ZbRDN{0z6$g2DwCi4VF_~tu!y#}0?MsFRl3$0kpB%dqyibU@*^iF>Cq>vpu>O%-Z3_5xlaZ@<l^ho70qauUrm~41j#}%YR"
    "fs{ut)_h(L_9qU~IJv?=Wn45|KvsDXs}$T5op4DHkvj)Zrb)v%rhHplxOo^cE7vYIL4EH_MKjoU(N~o5wSHgP|67DrxJMDS*wKD8"
    "B6Kij5^<eg=b~x<<!)TS2grQ4EHLf;s^D->#;Bh|X&F8Bz!Z>B!Q%4Sim>_)gAo)&VN4-U5SF(&LTDaM9@Aa;Ec~I5|JZ>kjwOf<"
    "if?fZH_^b<pESij+-7D4XV@-ohrykoyrQxf7DFS*jKNjkQqcJ(DX&c=C8<h7P|l;^rJ?kNXqfOTO6?w<J;7=1edLBZ<`;xajHS`_"
    "wp4rtXucb&1tFh1K}J`O8#whmik-vJh#(R!{8g39=(-t^`tquggxaVMH7B;Z=ipLW1ipWFy=cw1P*lV5I=nbu{!b?Z;W*~*j4<dD"
    "T+{E^^~QPwliRc`mEUbAj=W81CY(kzkt&Z|*AhTPvFF=Dlj`E{{fN&3rCcT(F_xtSqHj_Jg!V#Md?OLCcE?|j@mi5d@qiMUfNki7"
    "Dh41RG0Mfuc`is8yYqxQ<}k(8p$r1>!eR-<XT>wmZAf-U;v_2f3pNk2<+c+pGJ~;R{n8iP?v>_Hs8UFwQ--@)oD00!gT;on+oA(q"
    "5m8&wV-mL)vjl?pm0m&}dSn4d?DpFYQ>pMB;SRt%u$ooU&@*d~MG?d8niiWI<=V{+Q_%vg(CV1o<an(!E7D9|+L+T`toiKqm@$Pr"
    "O@7>tP-Moz1X`>}Y!D(hO_@;#vZt~2c!F$UYU}qr_1cA|&>|QYTPVFfo0<CyQl>98n#_2aSg3Mk=5AamIv|6DFmO*ayr;B;2G9Zs"
    "W-ObwSb0>y`Cc=>LlPEWU9D-vQU$hp#|dw4g(ks`1orJQbUv(0y<ncQ6)%esq1D0+i*PUgtpn)%)33)gwms?-7U^G11_bzowwP)W"
    ")mJR-lgsCbt9i+MY%!ZjO`kerpY#P6MYzv3!>c4=oB*cDH<wTEV5_Ya$7%4PqJxgH#|PEm>P>&NLG4U%!7=YC<gmExEKCbUMVltR"
    "KjReZo!@%D0v}Ja4B8x?5$>M2^j8$G3)Pi`7$>GK3Q%~~t%N_v!a69Ipa?^uNWX!8PnY7w#3z0H4?d*nip`D^bVg)du^nkSW0c2e"
    "m>C5zv{pvQV(>caq+I>otbA=Z5@&P>T|8%euFXTzs$7sz^llkRCTxz2KK6)vpa5P_0j6&TZ(HTIr2kbs8!s>I+DHSF0_*ph&jG_t"
    "<VW6YhG~T}mFGPp_*M|uj59!z^|`P&ixnW6QLRo>s8rts2?70!JHCrG<DVWk_OFbYE*N89wJb(vgg^CGCru^7$vV3Zx3r45>7L3Z"
    "aVfZ?tUV&m8SY}uSHdJ6!!D++1|mgG(7jcB-Y{9lR;%erolZGgG6V376X3LCbi~(~kWV**Y<$HB?`tnqDrrjp(FYXr75h!X8tumw"
    "LOiAqEFlCBJlJS=gx8f71_JL&6<YGUdWSN{Tka7LB48@^>M5ti+PyJ^tt5a>)S1s-m7Z9Ghy<6iN{85#Ry}O-0cWpomm;nCUM6N`"
    "d2Z_&BT$mT##TgiV=YEsdC@Ore<bSkzd9UM#IYi+bqmJ|97gbr`c8nMzV)8Uy$p+f{!n~wTCH$MQQmh~KlBWCWkw+AKhg%)Qz;e="
    "vYZWcQS3U<$FuqPuqm4Ad9<arFhq#jU=Y-TN|OC<N<}d7RfW-c!wo=RYAFv0OJpn5Bnn(6Zgxrob-0RBZE)~ZZB(J@q9a6~b?P?j"
    "-~7dzZ}TnMpOwBG+)oozPB<RXwUI^;4c9r<`8eipM#Kkk(9YE{<AYIvcIr2YGk&|wx_9i-I&4i*talc<k8&KQ^#XMg>XE|6unQr("
    ">LwEGf^#iluIMqN*jTl$pojk^ebK*TwDd?r`V5RNV2(kvN_PYv&)qNWBEn;lx-jGB0vMf%WvDPIZ6XWuJxJu<QBRa(?HNc!v6wjo"
    "G8KZ0Ss8~rrHa~;qJw@5c9j_r-N<;`5+X<R9lqi&p7T!*LVtE5n~3zqDB59_nY*JLnZSZ#`Sk6ED%RIe$9Se^U{$S#0brs_UEkhb"
    "toe=$AMFl*<Z}ZZu4U8w;*P`VDrl7qBnX8E6&<xhDWIYjM<1V1qr<77UcK9=Jc$=htz1L%YirAhNiU%S+mH&wwvomzM2pfgbuzoA"
    "EzGTvPkV$O^6Xo$<${96@^%BoRL<;f$B|v14_-NIUjd6}zXd|0J!R(BQ4u+|oiR0ia<>G!yb=!9j`3p6*YrpK{hUuhcP`<4j`~Da"
    "bZ}xBB{GeMxe(|U1{3_)dd-aaXJymSt1_#pA~zRR_4r&?&=_R2jIyQ?wr|C@NZ+9{*VfZCp%RxVc84u46*I<>qH8o6l!Shs?K)62"
    "jC`({{>I#Os6F2^SsfMOjX=*>+HDps>BMBjxUn@YTRg59{7oaF8Bb`s@pp09Wqp6o<kC~g&;E$<xmlE`I{hpRN2NvoKZ+?e;l^1a"
    "V`*)Q?1(<|x%ELd9QT;ve5-JCul5{I#k0yBEgq9dA`?#arZ5hiv|4Wv@K)H@ArZ!13VEgqn{+ytC-<U=Wq;3+xb%-O<Z;f>nwVQo"
    ")E;uu>(eP=lp|C0C;hXbl|Z21)hdz4fhg<o0?vVJ+3z)9rYeYzC)W+jNG}quNyjDRaP>p(RjEVv4%u9j8TKsPc44B2$F}Rp)gv>M"
    "m>tZeiMoz}cOVtdp;+si!ib;q=IQ*a8i>IN2ZRA2<*H8QE2pEg$`EaDRmUl#)1t#61S7m@(9qndt|BBjPF?kT%|AVU?VO`)F5=(g"
    "6hA>a8qq9KkcQJ|($N?SvB+zAJ?+*`S=h$!#BinJUDvs}*g5I#=)r-I<_N!?vgBx`kT^$}zIR5R28E%PeaCg?=xbY4eRo{i#JDR`"
    "l!)Jw#O^iuWah*la6mU5nu6GV5J(BR3By~3J#3*uF4N(ACQOR-vVbnqb7@87MCx}kd57&#w+To6tte@1R{|w1=I4Hl=tTt#B7jcJ"
    "#7uowQ6CZNyy#iBcG|d@@-H_5@45Lt(yoCTm|p_z?R2)$kzBZ;&5ASDW8|qEtpLRIR}tN(RZ=mraI2ZOfS*w3_nNN_6W9Nd=Hw6q"
    "hC>ZJAOxmw?8u053HFZThcLM%T$X-=jv8_9K4YJR<HWb(oc5%DWVj4h>I$;mD2UaCmRD%FzKN&oT(J?G!sknr$2GK&Yr!%eDyAev"
    "4RA8&f3akG+B^}02sg1Ozt}r44o$0zfJ+dNz*@Pm$%Hl0R?>I(7Vr9sshTgRf7#a-I|)`9MEpk><lCXj{SBdU?|dX|Hm>B(CZ_oi"
    "v_M}RY1OXK&oEat%tGPtCb*AQa^-@f>WN*+>)WH#<wxu?JN#H6ih_iD*-8i5xfiA^pogKcS)W2@O?fuzcSzXND8@$D`a#HsTFAOs"
    "vcBDGs9b1G)Z3$?2nx4@`rmAJ(AVFO)K+OC8i;j3^@p;&6S~?>lo%zt*)AUQxt&GtsGR8$eXyl~Ma0adXF~7N@1WF_CgRZwUfOK}"
    "Iu;{aW#cvDm^#l?d)N0}LyqC^Vol(%;s>asX_Z$|#?f{c7VXC3SBl$L2^bHGDeIUmoqB!biT455;iV`geZz@AS5atx#kgq>Yhd~r"
    "J#7INBwtrXiHRasRKSXRV$G?4<tBnaGoSG%`3)tFi!~oOK|@jEUQS1>PQ>rGNi%NL`;->qG^GlB(srFT=S2KfG%gFi!c*4Z=DV77"
    "vF3xklZi$7jgiDzTEd(V-J#(ew?-UAg9^}Kf_)x4dSICX&Z1TuNDxdcjc+v>_h)5FYpKP95Y7-`i`XlQil<h4)GHJu+=|z6bfl|7"
    "*0I5=-g_$mHGB~}_uI^i$W>m~-pn3y#{(cVbit?<ASkvfdI&Vo9*j|#m}^H)8j~YKx27NZ#DZ@0>VL2K7{gdx4?SLPprBDPp%`Kh"
    "8wV6epCb<6sWQya7;PVRVjZ_uvzxL%Ns$(=i|4GbasXzy$iIngPJ%6>b_-eIDXi=klkZFd<Ifq%`JV5&=v5&JDPS~OYoobXGyJWI"
    "q$NcKF`}*fqD`fTbllO|*b!;BVsc74(N{rVZDd79{X<c2pI$)M;=FME6>5448R-nOq8ElI<!T*hQJL|v7%yrsaj?V*QRE~k+A89t"
    "e@bDHRtc;96Z%E3)Zeh4USB${zpziP`lm9<qD9HVnr7qXBjekxeB4v7Vbg!FO~bTGnRMfTc)qbrd9*~ROO#V)ME_{?P7S6qWNQ4T"
    "*~}T5l(kjL{%JNWY*hhDVH%NC9n)?V<j1&J^G_)nrvuW;_Yb+hX2g^Z01~bqEi&d%aG41D{KcXAt*GDed8dMLF4wBSm?BETic?qs"
    "A=C7&grzl~4-%+@VzIGk{x5aYbr+`4r2wTk<`Sf1E_mz$^<BhtH+BA2rVC`7-7I~E-%EP`r)!4zDOc?<OkoTJFv7qwWc;YPZ$7?="
    "Ly1WMK5ay0r%p?ugtlU{3sCdDX87x>C}@W1IsNWlk=Y_+9qWo^b8F`rfAd4xK$#$C0}xD<>KIKug5labh~Iv%`P3%q{m<rTpNb4Q"
    ")0Yx1qlAJ>V(fL~$h0J4&eH_#Y_mw%ippgUWD}gAj6lH!xGax(xQHi{#>`5IL5jBIEw)1gvuIyKdmU{xuMn)vtNe>nH=V;>(aJ8Y"
    "^_byV%Y9!|yhDY;3qCSuoH4LQCC7E!#E<fv;<>fP&7~-kJX0Z4z?DH2Cb|`^8?Nj<4(&H{vE(C`DYEGa(8;i0VSEWeUd0Hrtw_NX"
    "auk$Z=0^+YXEF+RPe1Uh3TX**%nLYNUa`#TkiJPrlR*erpLQrXInzW5xB~%I9T?FS*|I`g(III*i#tG94(14MT`>9bCO=e;^+(-A"
    "k@^-C0(hJ|IjcRVL_!06?=u-~`-HHCkj_x#UYL@cV!3`or(C?<ptDIAwV1_4m9RcxJ?gAhM>}d%SXI<!UbJBvETG+{o}8ALjYW;P"
    "t1v+o>hz3eh`G}d*gA&uVSaDB`nt*Jj}q+Cn#<>i*~4COGlQt~so#vI$ssIan2{2La0pTML(fy*Xf`ajmB{ON`l@e)jxONI{O^Aa"
    "rQeL5$Pn+XiX$QEfKv<AZ<Dk=kGsA2%${gerp!iQ6jS*`U*qE4KKFoJt<czkd}*C{K9~qWXsI&tH3~O^H3|7S^b5ySXq{Nu4JH*U"
    "`nNhIXwQAG`8vxobAT)6&Bl8K4w)J7v<D-pQv{cg)y76{P`(HoTZta&-7SoyZf%srJ)AHBYS%uYCmYJQFutqtb^7(Rr`wx5xz|q?"
    ";oN#Ey%^Of$tnM%#hp5aMA7G(=&ReYi2?(zMZmUlk{}`01_H86>DFF!S=Oh0BObk!qftO`XF^X0@|bok9ZIp3#fXX%C+bb_3m16("
    "Qw@xyaP9<VMGO0d$h=tI!{0LsRHSz87;nWgXmMAT$M<|M?Mlzwgeiyw;3Y+ef;e@f;)R!Zhk1JUt+#|7;HyP-RH8uZdkpQ%CB9=L"
    "K7=U<$}g-p2bvHG1u6v{ZQ5faVW|o<?QlN?xW~ns@g1k2Igmk3WR%X36~hiYQA#UJL@x3ueWfw5wj0@={U8Ra{1<CjeUEbSJ>R-V"
    "#8-mOX@RDG#7tqDb>K45nh^vO&d@;@15+eO*m((1rB$*sS`&at@vdE*$%i3ecU05W^>eL{{uQl&=j!n5XZ$-53Ye&wqp#n#VGcX+"
    "VyJ~;e6e~}+WkU8+Fd10q)JR!J5xT3_>!Mdgpl?3F4lY?p=Gv@!ujzU=UNy=$<0vD?Be9wQ;aD`We`E$BHEddTR?KAtkQ>KvVcJP"
    "wm$ZM*2he9J8A%y=6MWoQJCJoahD7Y@H3k11=<u6xO_AnpInGH0^hT3#!Kh7dzF8!k0U+fI6Mo(3*IvqroHEPM%^_x!9Nk{FoXa^"
    "!V3PlxU32*X|LiJrt_hFb#XbNIQ*nZAfad5v}38R7bTXbvppG>>j7^*A^FZE(+=9Of}?GC<WiX;vGcT<)s-(^t$V|Ou!e`o&{9c9"
    "oC4qyWmas!vcpbALmfv~`;3fN3FwSynz%$pq_hW7+%fZanO79+AF9y$LnHcCjG!cKTLIi<n5fnhSDJ!cpt~hh`kTkbXcLVxrhE5r"
    "%FM`ImG3oQi%Vn)j%H|Uh}9twfIb;q(QNe039uq=NLYA*0)?iC-5X6qVwHLY;MBb@w$Ov{kxM$mws3!|lO~?2@5NLe=A>pa>4))?"
    "fFE7!`X`#7o8fS}U}h2PP@4OWcK*+7*`s>K{Z^}U^t|@ssy&Ic7lfyJ>o_(f{V!hR5yQRSl{|M$C}QdTibNTa5qGhseJw6&gonr|"
    "`z7%MJE_%Y1P+CYNs0I^(i`ijPX+}83l;97QOX0_&jOq^TpX46z7j{Pz*MnLwzW1iL1){s527_4=U1adDJI<P5xC$o+6ezC2`J7o"
    "XrkOL40f??v-|TNNyk>q59{>b?T)?nk~g}yMNmgCS^sg|eT)bP))YxG0}s(6#N5YW8L3~a`MRWH@5uKYIuoH)0Sz$%9ZoSPvp3=p"
    "D{MvcuoiPOia4zy*K%xy;n7pEDa?yK^v++1NyU5`{=a|dowh9njDJiGaa5#%mEPK|Ln9nnhwVECo$@BMR=`fUy>2EN_08s>v%@o|"
    "a&e~1t6!b<i!yO$I;4b>2Tju+7jyK&=sC|Qqv1ZS-JXz`WUM?drAa|48=Qziz1ZxJW~6uU1v>Vz-BgA_l0g70>J;gurfEzNB<tP`"
    "RQ@tH8fz*?C==;0mOLnq)cD=bg`w&z4`zO3&{kciuZY1eu2?anAGK>^)6~Bq8yJ}DG|;I-JzF6}NO%VQcJ8~i%D+z5PA@l>qiRP>"
    "P4|4nU8#uLV=9ZsYP6%F)R8jsjtyOn#wC!*l);rI7k%uDHDAq0EJB$3v4fd(a|Dd&Tfk{pVP)Z)K8$Il(8ZN~5Q?MH^W&tb*PYPz"
    "Ef;G(ZENW`_E>UJ;QmEBB?^ZM?%T`|Q3tWqhJ#%|?G5F3QAqKUhEYrM?7KaM;J?>&=AC@S{5ar;w>zp8ZEfO-?`F_MQR_6smb{aG"
    "r?6*JY(e<eGig)sC=dL47ti@uBE;(nbJgmQ>GV;Q0y{jWxM&uyBxO^nKbEo-zb5+VM{EgQ9qV_+RiR$2`IZR1qCmJ&$<fcekXH1="
    "ai&FZLLUM&%LJp}6>FQin(|dtzzwbJLZKJYL!F7C(~K924v6p<b(@PdU++d-#LxLsO29j83K~QGTF{rsTM<_&L_kCk&{xJvFgjB3"
    "6_8-x3n7>=`nQ_2f2W@<>HtSgagwL%Sh?yCqX{GxK{;BooWqGic`au#r<%G@edtNa>o{@l+}DL%1SWWos&5VkCZtqk#<{Ci=GuBV"
    "R2EUcOpfUm=XIVv_nTI+rV!Pl&=w*PoP_!ff$TFsC|p`|rNKp{IAd|(Rx(SkxCDZOeg|PkJsF}KC5usqmh~pQ_A2e{hyr;8^T+p^"
    "e?!5D-2(L#^(rRm-ZW8640ZF5$A~-nJ6LHdPzr>=7DmNpT(77rUk(ITE<*I5s7Ao>8W^ojgCAEGT(1I@0vy86mJ1Z0g_?zY^l)P%"
    "5)zfPXVOMkekaj2s;u8@{=@o62#dBeG1sM+3hScj0gZoU3AaxZMlp1*3%M+@2|lsjx7&>Tbd*+Tu((+Bxj2dzFQIZ$?u-p_K{N!@"
    ")OoQ~-Xrofg4hl9OmRcU77}{Kv{GAr;82wFx4@TwXNsKzTtJKT@5VK4%!S--!Srs_WE8kbHZ5V!Ed(|ZW3V2p+KB_MaIh%L&2zdZ"
    "<thp(ARG~^K(D2cR_>;V<tp5xa;{i}uj|-Y#gCi@s#*FU5F+a7%ncPYzMT}9#eVL(DEdP<74tV^G9oz5cs#Vq>Am5qj<TWlG?x^5"
    "xkBzkSP|hV)I_ne{hqY-?(LPVS;>{htW?1FvEIeA%3uf`4WqjyiW9^&BV{&cP}`olIfWy!YM}rzKZV|lHQ#9*{hpBt*1d+96&CG6"
    "7n=qhgA}RqOzvDeg-TsyD!ETYQ!2+sOkCNyEkyG!)_l3VPZ{7IG5x}cvmjDNWdIMUCT^fSu6QH5&?)5!ux};4seCt#HgjU2@ai$$"
    "3*7O51Xpz7LuZO$j-kgZDc~?tSMXd^vT`B_)CUD<1SLfg1@v`rOF))0UDyRsDtO4BPPsk!NwHj~Qecz$v#RqEhW_L!xT-MKu(sn8"
    "yaas9w8h1o#O3uL9+y~iv4#9Qy~tcHwg{MgV06;OJRPl}B2M~~2jY=M;H)qD&4jE_nnDqn&I=OInP&X1*!tH%-E+tTqLFk&a!GF+"
    "T3t-u(4Ea3s1UODi7!fIsDWa&q<37E8wz@slUm)kCYjcJd_d7113u{p;*g%`u+iSIle^P^Y?R|dqz^&MffP+ewq<U_a}^SNV9Y9A"
    "z{J<fT>K@|<tvC##8(&rO{gV_=T>ed1?mMEa+vj~l%V2=fpQ51d+-Wo8w!->V$G)~CmhAkaP&BF{^Eug)sldsI5%!y$D$(B;L!D<"
    "s{%z|Upiw@D<UEW3%$tK?#(GxU?XLm7~)z1>oa^Vk~sEYR=-VLOq3y&<>~<2iEk+bSbYLi`}FsP6DgZ~ulbbg63&^Krpba{bInP4"
    "MWLWjt>n5wuUPDf{-%k$iV*@AJkuj0=AN`~nXhA*=3-6%cIgQL(8PfQqrw0C408_=LYjybmljs=lRUBzTb&rBRXr{Ge&RpaOY2wZ"
    "7i+##If79cdmhDYY!WEgYs1WH>AUZsF=cF&x#?D<6{;!zP9Y#Oarat{GR1J;`Px7EI>Iy}RhuE;eDprO4xe7+rtjBRyzXb*Mj;8!"
    "yM~yNLWp=W<Ddz(ZQe)j7i+qgmg{?QPl8Zj69{AUyNJ}-?Q|-{YaEvZ2nGtzbDL`MR}zP?s0gk!sB-jg^<)29zFP_<2@^k$(M=sX"
    "1bppMg(B-d{oxA@II^WU>JqT;Q_-#rw;Kv8B3#1Wrd_Q0C|!CZ3>_|J_x-D-pwsD1pt)fo%PvZz+^VyjoHnFBjkv5bhgHH5_0yX`"
    "<8ALe=0bW%JIE&?4cotG`n2ZTP=OxKkF`c!v_AWWJFViT_?&S@bWSX4;fTZ)I@5l=x5*@|<vvjCRQt%Li+%Suf##wAOt^5ntDvBd"
    "Q}R~K5Us~AThXoBhRK+7l}$RFKW-P|Zw-Fyn6LU4=0W@A71QI=UT1os>W>=GsqIo>&-!bc${C~Qc1A&KQ)Zhw-UGKky$W=~oBqM^"
    "opN8%2z<%#LPEgr$R##EwUwhsDRcN9l>{-yvq<k+T!2?oB38c2UYG~n>cW(1z1ZuID4AZDVJPZ!HRIoyLHV9;R3_leK+DuBA6Dea"
    "g27r9dvz?L1rQX<m3PaI-{!#nwT$<OjMV^y(z|mWqhIm0M%OYrwRXnQMWd8}jS$9|NhDy?A=c<%4NAH*cTvoX@A)(z>GM^0Wo?h6"
    "+&kq&rvV+`NdQamoa?fIiJuW;5%OymyYkkfUKSS7iWa8CZs~eaW_QXoRq*yA-u;;|z^AC*7T4(bZTb@z63*DyD^&&RR7tE88}N$o"
    "Io1fM#74TtA`LLXyK%7x{?sP52iy^j5b;S<f87!MT3oi9q<6gmoJ*vHDS#x(CFb4~;hYy7F-8Bh=g#tG&A+{#7<ERMwIKjtV6j1a"
    "*r2VWsK1Qho_;Z$AQHb+G;uSuvJlV4Pg&@Ge(S(!&F9X-{Az%ZtppSj{(ww2;b)@s!i|L&$)}T7jFy3Dn}Se@<0_*GgQ6&I_QhkC"
    "x653g@A#R0|4jXB*{=EcxeC501StD3qNt3rOBcFtO!DjZ+GN!M(}O0rbVi2^eG81Ij{H6R>R+3A%aw-#9#VY3;%y0mM|x|hJTInq"
    "1VamUXHl`n)Lv^7^=Ue|0bgVRny6ps`zAV=@TZZZM`hr)iA|v7O&6ORO63V2%E%OuN!yP>+|-YxANs4Eh=7JEH>%&m4Fa<9?>Pab"
    "+5>@Nq=L7CTof|o_~_%>fqrun8K9lI#o+OL4<Rx~Xt>P-hV*X(2SvMAVxZ=q0hcP)T_;6)L2ff$FKpsA+G+bMSmFpK<;EmMz%)=S"
    "Bt@NfzZP~5-)lalI~*>5W|`Be{^B8u5>YIa)x`4yL#bZqXyvg}M~pYj<fcxuc*w$r8r3I%vE(1CNik7Cp?w8$U;TN7C~m^U5K<o-"
    "YmlY5?YZm>v=}VGvvi~?m>u<q8<dMRpUYNM_aa$(LiCXc@6mW2-(9J7>&&*+(1k;{GgpBug)dj)CPFJ|>+6oq_4CD=^%3ggKFHi0"
    ">G0|n)AU!YsW(4VUfB<}2TdTz9t!418nrjZscQ;ZC~Are*4jITIPrla_S3Vn6hpoW2_ie&^l!r&t?8fPpEG%nLbrHlfvnNlu3XSy"
    "sAyrMPKwa|WWHZZSlk+R(Pr*Cy!yRpF}nx@@62?<D3zuGr5;){Xo$x>?Q||7VF^>!uffCt`z52kr*P#@lcbPuw+;RrF4lY|RFrz+"
    "kF`^ZN;XRgz4(2)0ldlBd}uWYoHD>TDu;?hRa@5_RdS0Upieg__H{sajGzQ=Sh*;%N;1lHg2_Tu41<e^PaTR%m*@@@BXHr-R#w(R"
    "y;Q$eTf!2{e5arIUK}w>V9O~EO-El@bK3JLM0w@WpuD>x2G@D%nR5x-cwF*^KD7~V*wnVgF)q$zd<K}S4S*mBIGtuZ?Bdn}Y2=vo"
    "(6#FHjYa?e#*(xmqa+Y9cT%7jt0<-5i}1bVTU$c6<_wEi(?yCp*K#P-*iLH+mannXs!BaChKvxDu$2`$Yn{>Firj-pt+`k;JeM2X"
    "2?JuIG9yz5TR41F{e1*MsR1cn=W_6Rz%IyaWcrk`^xtAa&fu8PeuvnYUJENhZgYgwltq|k<;+dYghv!wDRal;T`KM(23#_F``a!+"
    "jUH5#qywbH0Wa2kng*CBO&R4hgN?=Xn58h&%S|Aj9JQxm@gUZ*r@b#SxtW?r6V*<$7-MJ`MlRYNPdy7IgoD(+f99`f!o$+6f<f#;"
    "D0XTCGvM6Bp-_L)Mu^4eBMd4*og22?70rYGVYe88)+_55Tm1<midsw&qVT3jD~j2bK|OBmVhMxM%v0YOT0f&ak4o>H7S$d7k|QWi"
    "ql7Uo3R3YAxQSWe@-&W!8Y%-M6j!i95q#)<cLwT|hga5SC?2ym!HTUfqXf%rzeuCiRx#fSz{pxY_Fk#xGenpu<uS}=*+@c4sApqA"
    "2#7xV8L3W1Xp7^Y*MYETvFlarRO6oEV$FAYM<>;useKXr)DZI;MQ|qL8ylRyrOvj)Bq=K+h`cuJf;R3{9$z&_gY9e=d;QPx7}?Kb"
    "LFFCJt(h0Q(LfE}Q?V8Z=NcdLL!n5EC6Z1ZR#o^cmq@>NN#-J4CjTlrGv!T|XT%l$AuP>>qz<c|`KOX|`XEV(ik1q>`j3TN$(~$O"
    "H?E}OMI1@Ny>TQkVS2-c)1OMM<+e$iop6K2X=4bC@G3gkhrqnzKH8-fTo)A#MdC!C@j|SD(H~li&G$?LHJ#sIlsl?(!<F{5bJG(l"
    "dZd#pHl(~<ht=uE@v;oOTy@TcGsV_%GS=QOP8kNh;_two{sIY&2G|R%i3y4?tC5m%h;4fJ;`qb)GMLS!2&-dU+BRD$0QYDR>|951"
    "`(n-4(m+wY5b(qr2DV#Hx!Fg12t(65MZ|8P_njG$Hb&GH`ZEXh13%Cn674HD&-skxReEaCaB+LAs?e_J?fQzsec4;9zevoXD4o;v"
    "-n~9&WYhvSrajlP2p{(x-?ZeNp+}58czZF@;9tX8tOIFVwKqTXXc&s{=q#HPn31ki^ji&&H_;;O{GzDu5Qyw^7)1}}RP=YnsWXWT"
    "Z<-}w6fbVl@;g#zPoFuG<!<5#81-kIs}xbU+-TRJg<$IKQoYv4)PGgpPRWWv3#6g6&@i$3Uh}!tLNG5_&M|GveYqYsijiB)U9?7j"
    "CHSb1rJS$D>k?b`7^R4SQQ{wnhv1TNK@oC%gNL5-%$gCBmw+%8Y*BdbAax?pjWHhcm919}lvgD8Q8ciipeabySHq+<U99;eJ-sDQ"
    "o4B?D!fH&i;}{*VH^|x2^{RJF-ZNB;l<`+2$_Zwo;74}>0q8qM=C?u9(bdAujegV0-QWMG?97rx$(7@}7k<+u%)@dp$|w^_Gm)nC"
    "%Mb2L)cp{ZSrW<Due<A_G82e!4@9_AB8xqQ(88jzxkcn!1eiq(P`-DOh&{E-kc3QBqmvywd|30rS_=1ylw3wSznNtU9Q1@Xr-Glr"
    "ilLb#i#XsDFPd(v*zFub81yP?<MyD`A3wa>XIvzJwJMia*ftvlb*Cp06H9()q56fDSP@iR`h%UYyrb#HJEt9#>yBLv+%_es_t2AX"
    "uciJ>Wo;&qn_#qXe{v<@3celwYf7WDT1E5$GZoWR2q=b0vhD=c1UMlMw10kB^J@HISw*-jm;rJB;RMGW8Xg@5X4sfHc7cH@BS3w}"
    "q;=xe#ym%&7Fgds5dDu8D@AgTD+QAD9koaX@tlYz8fS>tRq^aaJA%=<(1tN3F^(6F$V)>PHB<kxAJ)89^gdx_qLdlYB;OPk6>?%>"
    "@U@TGktxKDuF#B9l*xtUCc1@s&H5Mg)Wc&*@V@5t5{tq^1+mbM&Kh^SN`I*W3|2#o#-q$b-(S!lukA!DyY^B=_slh6Nw@gK>%*G)"
    "Z(Alx6?#46*yiB%Macjk@(~t;ZF)K`%~R5wC#U#TMRFaAc4!k-hAiIKbl3hBS8zC=e&@Q1XuIPw27H5R8yeNsDH1v*>FQDHB|or{"
    "H+MT+`g1S!;WDy>+pu&T%z*oug5X#*%<j;+Qy+M`h=b7afnK-YT3)=?CX{IAa$KV~AH@dWvr2hXwzOSvyHgnAvP;Zasbu6TSeJwo"
    "GCH0$2@lhw%ZOaXEfgM)@5@5)LZG2(*NqQrKH;_oeC@ew!J({(FDddE9Rw^6%CH?<Hgqnzq_fNM>3rVWZ)@X@9eQ^`C-lRsy|V@)"
    "x%E`?Mn1$fw$U(+;4Eg!?D%?dL=><b(l0;)MZ}pJzsMY7g7`1Z4{JWhGuL9zQ0G7^+UlHClq>KnL?VT2L`Nf$u*sSR-4bo7ZigVg"
    "acfNx*3$Je??vx&=NlCGvSIMvwyq%R+L}Q$-ZnjpK5O2Ns|8Wh$jPZ=lp%P8;Vkb$=x{Ns_~lNi<U6G+Agn(QQzz5B*qYOcv^gt_"
    "40@or)N@TQUC4J1mr;9)D5NYm(bZu(y`$0eVa@06OmwkCkh^we#!a~3(oz_=G}UIsH7wQjOi{2%Cs(*Z^o41b>FG--lOE*#VQQ}2"
    "X-nly@jqaKK~CNxFoPLuNDLL_rVz^|;8wKq2zre)l-m$oV`^QgT3nXdo)2N@NuS}*X#1aOWS^EmMnO+QAOX|J;`i9YDgFdL@!)nc"
    ")@iAlM#U<&w9rI~{lmL`6d!R<il>*STKa`)S=0*?4cIX%!tHHwG3cj>K2(pz6PGKY37pu8va*8zoF6K~_0n;Mz60YbdYL$22;b%u"
    ">14KgJ{`X?Pjx?wYz73giwJRzp|wQYFSn%YxPZQ|>7IU*RPi|#;}parIaiow7QS2C9ghuRu#oAsC|M9i4AkQK>cpB7DXgIdmfeS*"
    "il2#gLB8zjQMN3DYmB-7;iH!*4(n}uMGav~f!|as{*Y0?=v)O3`cT9-%TR*<u;!h-M8bx$p76O}nW}qXtYFtcDWy&r??i%-lE@UR"
    "?+Lwm6d-k6>FHqz=>r6-&)5J{JOTnoGD5K2Y+RuAR18YQ$^>NLR+#~Sq<Cbq8I<MfUd)3Ecz4Va^mFaQnm?L8irvnAxp}D%t<n)L"
    "<}`F!v$9o|W5hVZWs(^fa*G0lD&D+t*A;Ud3>M<gt?X!+*ThUw`~Q8-ACdQ3-H~?o343!fq)n+&iQX#zitZqVjw8`zF4q<Dk8bJT"
    "@9ev76*{SNe`qwX&)fhqhVER#2OC!*x8mf$5WE}M!uE)$wL9hVuK(!Opfp02R7VjVAU$Z-(|%a<8Z3$894Ey&6_E28Iq&ue3(<J2"
    "^QvE@&REVk74LDziMFC%tkLtKacRx|kQlDo0M)#5HeTm~$}86{u`AEzYZprn8T$>-SZ5Rx^%T?v1%eJ7idXs@df(Jc-vLd%2{7Yb"
    "%{cY4$8Ef{L3mo)zNKc6%R1vu*hVoP;TjX7^ybxRgg<ly^<IP*KdgBhIk1^OgQ>z~2~%iMb$ZC5Zc5;$2<HoP)l=F*^eif-=sn}B"
    "j2+%uHH{uV1nGR6<qA6`9VrJRbc9*XBDt4|d2lv{5jA}4vl3$qY-##Px^@xvWu7VE^+EDjG4ScAx7j+EzLYQP6^5@$A_+iZH@Ezo"
    "cr)TOtHLpdaDZvT%3;N$*l3O`>DcfFHxz$T!B4RI*L@f6NPVDlEKw>cf){|rXy+P!I|gY()ra5Iv{A$HWp}F_8Ik4?NkhOH|J+zm"
    "&U#<dKE@4|8Mr?}c!VecOxNLJdhr!k6r6fDLU7_CVo3^Wu%i_8i_U46&Klk1IvF3<{IRP|N{Kjl4iA9V&wGn(f%6^Li@J_jzHKPI"
    "Hfn4lioAQ;)*C`6G?{-es6fx@xi(Hg*@yuooh0gO<#@*ni0G4guSZdcOqi(&-zkHLlYx+;er)J)6dY=e-tvB5^Crz;O*l@a+99Ef"
    "foLBoQ4YhLm&ZkNC$xqUFtH02sb#83y@R%t!9=+aPwMY$-ljmt#1a5=M($bKu3iA%cds;5HV%`xJBtMIe-S@iIPC09<-*q85wtRN"
    "qrI>B(=3h(8j?rieQPKNI-w}REQK$@^ClqM4DJk0f4KH8qO$-7;kAOwukiLMzs3(|CExoV;T?y1F=`10dqO2a#PCo;Fr)DrVb~aR"
    "TTxm^CBxw1q`V<Cw${SRseD-TDZ7VbN=4&au!uAjJk(>?cA7;T@VHXx>2Th`gi2T+hpIDJQZ^;HuBf2zTt9rz^5c#wd}+>Hg)hC@"
    "urjs@oI*iAYSXTHD})zm!rx0^9r*{5r`8G*R#|Ash4-12aOaG((w}2Y27P;BD`8nh0;L<Pb8Bdfbtn>yna(`F(t&b=5FU6jDC>(p"
    "B5L`5trhN=vCjzkQ|a%SX6WN0pg)@?AdZ9*l=iq{12e_Ggwl+X)QE0l;moj-GiMU`{dn3NhOj6*aHi8quz>6|ZwbB`v0J4ACU<DA"
    "82gyAX<JQnM&hJ-cX@(BYYLU`Yu=KOq7H*v3JIZO2`TO{8PrE>Pf4YV>Q<Ul7+~9COEjuSkKPVGE{<t{22wO1)_lgP4W;qZKbtA$"
    "3(BNynZC+LXM*cxuXoW})?d&Lzr?+(r(A{4mIA-2NxzAEU(;W++bAdz>IWH{aA$rQjG!3x&;K+E!pKYeS9nriS^U^dhn`>?LR=%G"
    "uHYteAkEOJ5>kuxG~74spZa0V+wy`e&>?V>DwW{tQWEeC)gX^DV;j`Px)8*`nWS6a!f-8b+*20HF8Vuhe%S0!hhO$$_-DB0{bwHi"
    "dt`);SD(UWUgj7cTFPG9K7#ytIxds?5z<3qUk`!?2*UIQJBQM6EF9s(tN9V74+(oT&IhcExw}lSU!X%IS^S3_O%qB!JB#e>aNN?$"
    "PB0wAO*%8CVSQNh@!hkhMM966a8gpT_bb?0!^o{p9sZS8XV_IJU@~k~aABf%1!WICUW?cmw+~_9v2+TV2HI1lO>r?nq=nCIia{4b"
    "h<oVsGB>L<mgB18+zd`*z+sX802n9|e#l<-cFBlW&;v!OVxsBFz{5f*+H>HzJTP5&uNXSb(BE&=XGOnE#vwwR)}!}6-!qB*VHg(w"
    "AV#_n3<^lWI=v~b&9eS0^EON82%lWN2Z1}~b>9{<ENC6MWETd}?_zIS^T+0m-ZixMQMsobZ;oge(~f$a%rKMAQbcE8bOPM7D?_@a"
    "rpkrD#$r;~>_fP_BiNM93rzq%D!x{P6p%xzS(z)M2GWXG>cW~7yT3dWK0IypU8o9)GY>s=fA=-eLf4%F-v`3F+odDPVG-*<5_r)~"
    "%U!0@=VAb$_CIb7G2jcdhTCulalC}D{f9MgCx{4wL6m+C9O6MUEoG(pw2*x<%tm;i4@VOD^oGcB6wZ#T^~NM|!2{;Q@hm?jQ|(4Z"
    "PPB#e>Z0*@tLhC_?N#A|Fhs`j@OGQ~F=eaZC^%Wsrl6D=HZCNket5Uf=$bR927tw~eshEsg@F{Hkw~SQu=A<jRgNoYKyblmlWOA#"
    "lMMapQ+vF1^WN+b?m%>*ICFe?IyHYZNQ0sagBpi;iSw;WZyR(lp0=)CQQt>oPSG(7M_?SkKP-8BI+&?v3xRCg&LTWmn8r3E<MION"
    "5yn^WsIG{kTEDb;DY1A#cASl8HvU~bDcl)sd;%>|D54A=6l*fA2tK$n5{h_CO(+J{Ri<LU&g|Hm7}9Gw7Yhub3SGZPA$Of_g@BKL"
    "xgI0o$(kmz0cOXn<vI?<$f^j)VcMSZ>@5nx3wG18f+NNZL763fSn{F&G$L{(rWdwzXP%lOMAC0!12<%~ZlA0lm2YZhz<eb1d<uyX"
    "QD~)Cf%p8r<TcHr?f;l&2|_rU>qWm11;?GI4?z?~K|LSx>w0jkH=0<9!mAwuF=Dn#=!S;14{JW?Nt-~cB5>G}@^39dM%-L-AMeET"
    ">%=}!0XPdi*n<KSEq84=Cu+O}t4HTX@WYz!cEB-93#;y0s|)kbh6`}^SyaJ}9<BHvq@*l`*0G0vfo*|5i++$j(D&BUepvH11=V<j"
    "VP<uD5)}2t6w0iWIMZ$^(BD=ky6=M2OkQz2zN0T`*2(>L)$jKKL5oX24G8D{xgL=g5$0uLm>Es`d8(eRr8pz1(0anofSg`3(w%!4"
    "CaHtK7mA_p`lAZok7$lTE%!_BlYZuY4r-kg^z25m0C#!};^Iu`*&Ht&NU*_ZYP-fY0J1Me+XOq(($8i5{s3lOy261dIWQ3qr^lP2"
    "AvPdbHm4u4(@N!NU3C_xQ<V8TQnI-IF1%=ike#?+`w+LTvI}7+^PENb2N?;v>`h+86C;Qc&jg|XZ}-mcp{Om0vs3bc7K!CO^>1y="
    "_ci^aJ?X$+M;$(N5pM5LkPt&aRirsA+?ygxSsXJR@)DO$h7yzZMOCVt8w=hr(uWj(CwAdO?)3S=&<dRZ-ExM^Qp#CK1+8-<+&dR)"
    "4K1#5RA;+O@RL8%wCUZ|Kx^IxAD^}H6KpYzjg45L%T!-CF5xOef2L=!Ix>YG=;76h%%xWZ(s5eRH{Bl!g8pH}fwl*0C`2RVr0*y+"
    "6nq-hZ=wAtoHq3sxeR=_jiIkFO?-_-YKkw4q-%C&KR>K_9c#<!Zp24OpbRV3z3gT*oI=1HC<clCGQU+A`e?q`4onNp3~%VRQ3UG4"
    "eONPG&D0<`!Tb?7*<*>C)wznmX2Bn>q{WCjE-Q|@)hOqNh1(Ec^>1m<9c8YRKdhPms&Yi+<@JP+Buv>94inmQA`<}<c!AMCe+JIl"
    ";&cJ^g+ftlFiq<8PxSpitofW3diW}hX};_2_34qPjcE<U|G4m4;NZ!>gXh#4{?1n47{B1Aq>?LYH}M@m{W#O>L`LnR4}(_vq1$$V"
    "{UY5@D|Lq3JRJwjiaW-6OfLyU>79@ddyWEsp%630x{;DqJX6qX<-^8{w_zBQ3G<2m`zHfg21CoaHnB!Tu_d7k#R(<-gh!9aWT!&c"
    "O;R@B19va|XJz6kh?%D*veB0rA9l`@O+pd^Nf0R^y>@z|l+cx5lhCyQRbt4sb3<Vp%^Thszp~&|DLNNh(UP*aXwiFe_qX??WCR?*"
    "hebF{AuLy66ih7y??+N-?>a49=oJSD)+r3Q1?yZ93)jq6bil6<yY<c%qY{;w+ZGmceTndkH91AofkiAjVViP;*65Wf(oVM_M5^of"
    "TajoTW&QR0l~lM!?J*VZ{t|s!lNlp+9m2?Gz;YUH*RmK4UXFX>t6@tsjvEMlV;vjeoncZdAEM7Qw5#vKd>5r;5goi?{T!1xstwcv"
    "?swF>lsY`MoI*tPddH+%jxfUCPDI3fIF&#C%0Xy=f{sA`uxIG6G7fA~$EsyI;5N?OMdYdobBoiWlF5an6Svdv5EV4uCoV&9*Yy#f"
    "_hU&$!y>(CnIN{fDA0Sfxd{m97%7`gY%df8_ROh(3?{vSUD+g<`DM-csCLu%;PJ>Ar^{j*g$2^$j@S^zG`N;XmX2M<t;IXW%(xKK"
    "&W$wopp&hBSn>(%$(0(6`J)`@JMN!TFUxUmn75hJ3XJLf2d=G{Z|d}}>n%h<F+UADnF=HCb*tbn*b@)+N6m^lN1<;|h*dmk)E$db"
    "r=rm;oe=8i%N^_*gMb5GuABa~quKl(hQd?%6gJ2vpMg(d_|OPY%282iEUbosPB{+DTG!$B;+9^ppk9aCI<MGhhaS?t4|QnG^h3to"
    "X}gP2i3#&xK^uj7MLrRWfZS?oLCjb2#t4FnG{-^vL%|-D8=xFF7GodQ%(p96n*&k&mARKfHCK6gD2{?Sd*V(4RZh_yygv-nEqN{)"
    "6VU4B7p2}3!-q9*_irfagxj)XTo7aANFYlcNZ1>r^GRsV<7Z@%=%VjWxhBMg;jFNMMaBJn%{!GStmjWXXRS@EV-r6xIDa#hRrTLp"
    "$Bo0fJsHl4Mh}nGGeTusNY7H4ulf=n)+|3ZO$>41=}Kv+GaUvanOSGAZJM@YZ=vOJBt(llVvq_!&5A4x0;kAr${*hCQ~UHsH_x8|"
    "UMuD$`p48x(Cbo!5ck4#G2x24MHjeV>4rX4Y8)aFo%YV6e=~o0&TE5)Y?UD<DB96@rtmHNSCZupi_%@<J;x|QhQ5x1I|VKZk+#Ac"
    "=%?D+@BYJ@k5mfn+R1&Gw+YeyCFRIvVfpk!_L&rp<=Cw$L>pzL4)r?v62c|ixuR&vfMF2MoNO|$T`Le@EDpL$FQF^1zDv*O4}8*>"
    "SK!I{$d<aUZzv8@g#LqmlY>$z<d+>P!q3<`E3OtU4H#Uh%AR7jczM?MeG`9_^a^;6!8x6IrNC9uvCyuxsF5<dYfw?qQApTe=o#U{"
    "ejc})E~=tbtqk^VH5snqx+B;HiVZ0(^d`AtSb)@ma?Au7bIBU);v#5y(agb>Y(!sY3JrpYN`7yr#`)tRw?SOv4AlqP6c)NACb`vl"
    "*a(!w`bXiGrU}!LSQac9N<0<UmlbOr=t#Zq)p%hvR@u%Ro4_`6K?3;*h6uDOtc&e6nwy6HRJRLKQQ}Jl@o+Sv0tyg}6uNimH55Re"
    "4jY=)xnbmzS~**((XFAC%NR{t$Zw<>SA+2Yd9-~MF-C2cs!>{d31+nQuJRo2M6|021JUi0@oO8(@|i0QSGL#{L;JQXVQQZ$<_$6a"
    "B|@eEYxMYH@7o4#EW8QRhc)lyx1d`*dAcNARfJXrd+<gjt|Q97P}%lqAbNdarR~}Zy*9&~f9{1pt8X9n?b8K<<bR12pM>Ioo%*Vb"
    "j0x7EE4YN@F$JEirk(1Q#&<P3I}7c^<tzPyePsV(-(H;!i=88`Vh4lkf00CNmN(bXl2g<Pt{p#b@mwR)o-t`Xishm)*cQ!>VbA-T"
    "^%~|BoT-@7q4<syoeg@X7@d$UlEqp{=f`IFmOteQ+x2?}A>D#JB{QnD;eJ^2!9X=flaDCO>A6z`m{l1YL-4S+_B7omcD2@+VAatg"
    "?h1A}?$9fOXJX4t>A$X-%x%BT^xY8?xFxWtor_^^;m5^PjvMskSK&E?rUnMWIy9?!O0C?X;acKw(EG6Fb+ZQ<91|>7Q@Y3aBRf3)"
    "vLA}}Oj_x4_K41zw*q)M0^wU&=u6G5ZB^>KtT2T7iSZd78towqrc?seqaY^Y&;3ejQ(3HtrY!8jV~b+b!a+y7O;p^YuB9V7Z2qw3"
    "ofgEIfwLmg-fT?{r^-kNn8iF1Wh)d%>eTDag3e2rq>Av8k1zxfd(v@Q?VZlw33)U(4hH{_=aavqcSk>uY1|!@zy++8n-q7&_A7%!"
    "+AhVkBt9<uogz~_zKLYgnrnyE5fgYthvvhY_x4SsBBY}gj@#+pmCG2yl+JYDTtN@<L|4YSf~W6!_Mwj_)Rtz}JFMLLeW;S9I|0l~"
    "P4IDj1{5|ElE@bKgf0{=h>gyh&QxXG#ch!uO0xx{6VnxGLYTlF|7Fc*(77fYfj@nZNAgy`2Hl=Gt}8w-<EpTY!jO=_u;@wjHZXo^"
    "3kBf~scy&hXl=YBy%>t(q|(5LjdQ1lW2toQQXetwR9ecK5kkhSS^Qd1plwgva7SZXSU~@1FNI%J9tDV)7!~IJa6q0pnOXvSfDX~r"
    "`xGB9Vsk>|5$8>zfK=0%SQqyIuV2)Sdd94)JKO-!$-xut!;;Td$7HacUIk`iY7n(*gE1p&Jxdf&DR`lJxQkH4QXr_kuZ?`u9E|!y"
    "7O>0wu;$Z!M8gxEL;#J=2C71sd?_8e(UiXikB-mu9Cp$pVioZ8;IlDB6GmsaZhcwufzfDcyJHF?K2-}cJ4*I8EyeLl8?~Yf)^~-$"
    "1>iFEW@ZYN%iZbt;zq^W_cCj~o@hDLXv}eJ_kJ11cMawPj6$`L+eEBbi^R%YjNMttV+ihLWmo+I8QUu}`umV6`bAfz5QAFuC&VYW"
    "r%QAxr^F+x>alE{i7VfQO8(4@S#1%0(<2QdRybOJRm;0%pqSlrhmUV2CI?AWPYFp`pEkZa^xo_uh>QCmqgq9BJ4Ytk;6=Tav*{K="
    "L8*qmzASmG2HeTO#Ev~Ug;F+z_Hi>2sHeYLgiy=;M8iU3C!uyh;#g2HScn-R$oIac`*9#n{-_+fu5gUc@jhJ%%%=?_p5W=B%(xF&"
    "k4=rIM6#5bG3VLBd@bd=-o0-^zKO^M`$v94=UNngpmZnzh6|=3uHcCvp4$kkZRO;=D}A9J^}T}y1Lg{zLBaPmpWE${^ksAUpk)-U"
    "tV4+uO?~+CGk2B;9j|jFEvP_L12Ke-=NKwjHHs`4e0#W~Ki^kyrJ&zxl0skT#6`sT-%(G#reqQ2YKq<zlm%E@*ZWE1*hh#|32-vA"
    "FQ$@@uv|;1EizZ<qX3@v4%t>)`osHtE-@lExCwuiidIHx;*v}T4`uZ^N~_}2C_zNqN*qx%FOD;+9CTXfGTeGj#BchCHT{nkQ7YBp"
    "fh9ODe4KEPnG}0{isE|I2-{8qSZInfz_eomSt<qzXDSSf!`lx<?{Y<`6&;yEJA>b0CQdr@U5YfceT2!RvO;^^Vs=?{T0vpx2ZEx3"
    "(W_^$tAJzQk#l)OkE2l28E-)4NHDtW6mf9rdKFedZbODBK`%wPu$%EsPdSB`O0r`3Y2#s^Z2GX|Gvk@X-qM+e7RtjoT`*Z&YSv1D"
    "xkhD`QH}N#SC+VMj>^4b)4NdGEVP_(ja~eQHOq5v;;F~v1J>O<wa`IXP`MCI3#?88B)8*%;4-eCVv8Hxm}pnhU&W40@%x$&9`(ps"
    "AFB#&ij{{;H3|&2j0!<hnkmaD%2~bcxE;cD0$}vlL=o)4UGba(6wSS3;PT)+T;qkk*+PTMHcAIdPw?ofO_g#Hz%!enUBV)Q2p9ns"
    "Lav#PM#pHdT>tQ#@oM4Yn#^6OjyfUSgFeLAy<$<A2!{|5+!XwvI~BznJU+$BD&rWjdd!JM;y<kUSbf=}&VdNd!Xz^b9atr#x}4X3"
    "L=s_CiXfsyK2q1>c0Rj3^lF;|?eIq@?;To}Tk|G9pUJMKKPczPknF~mC{C)eP5h5m3Q?VeN(<x#C+58=(jwlliIZ`@1E5l`@Q$<b"
    "*iJfJi*X(7j2_G`kU`UiNi^0oiF{-LTej#2RJ5T=r%dLwsqAdw!<zZW%?c@W?2j-gQW9Q_NBcpKbfdmpQCQ3w;~~cDCp=fELA)(e"
    "K5e~Qn!R7wyhi5YT-;(YIa$!1iXA5|k4Yo`vPDP-dnILNypctyd%}r`deoUS>^j;_XX{;UF8mmLn_9G4vDZZX-)6B8uHLcHRfyY)"
    "4C3iiA%bjWL6<Hsw0Hn5_;-lbo>{XGYhF2Of9l@Ux{^EA*0Fa4YMJ|Msq+dEv7}a2sG}*X0r9w!R4|^o=GGM(-$O!u`WSsYs>e(R"
    "YBMJ+$EFq&$8vOK>dcnFZ>1~(O%*@ffn$8sqiA;G_GmEs_F>I`psvi^BN%$FBYEhN6LN?c*`+ONxJ|5je&rEQH*w>xZ>ET!fI>4O"
    "0uy24`eDhRt5l^(LV`Ar6-KWH3nWE?0c4GOYCXlkYKhzOMXPMKz2diuKSG;98#wrPRfcef#`RvcD$b>Nb!BBur=v@6o~IogJ(Rx0"
    "gHZ6!dc_STNGcm5WJ1?M?if9<FP*UUmcJEjqYq!ii1kB{<4v{U0E<O6Mq3P3rPOg{5Jz<|ncj|Bn*M7TONUMIOpFz}Gf>e%IZd1J"
    "eaRo~K>g(j(Wv)!M!V1(NW}=4CDO6@3%8gqBWM|$7?8sjJ~}#<490nit@Xpzc74V`ipN`$Nd<vYiq{jt5xj9N9zQ{W8+ECK2uZ*&"
    "LuVO1f{KBJVzf>~?dyX#6ei?X%dvz%g)XJesf6l#5@3x3<(B3`W@0lldu5W~Al+b_k$3E~#hKT_^C0B$AzGFzs-R64nQaduNf8w-"
    "lMo(t&S)PhN}s|0w)9l4OqZ31XcQA|6FA03PdP9UyWZElYViq;E0o)nR`u?L2$k6IG(!P;{At}m1Sv%1^-l1%sl~X<&NPRi&}f$J"
    "ACA|Z8nB+WZ+iyqx}r6+5nQ2j6+=BmkJSYkg(@n8Q@D%K*)?O>9&?&36s2EkzedY==eIF{(9cjw9$|On`_i)E+~I2f90!50k*IX0"
    "C$sgKxPMi|AQd=;9fhCtJH8viaJ763Wj;hY*tkf-iNkMH#Kb<WU@w04Q-Edn(XrVv%mHJm=ft=?gVj2_K^yPgWGCL4aauEl6VWN#"
    "MYzwBMy<e8RL-HPUR;=VLhTgAMv;rT7^d2O&PtuuK#S1j3?J5f!lgqH2V1-x8@((cPORLUK5LJP#slKaB%o5(vEy~!`Ds1HCQNyn"
    "P7IZ=5AXIT%Lgnujy{42r8cNrxN&bR0YzUrn(+gvO9UBW?!ycPKU=7FDH1V287ii!AJ)7&W!z_RK83w-9H)|bI=%EwDO1%pXhfv@"
    "4A~gPMO0q>(l2FjpbpETT>gGO^`3sJknlLgKJ4PKNYv*_G~kV-!!qf%TM2(y_H=PLmm5(=y@jUmuS_I3v_=dczULJUQ%ukyJ0?92"
    "lj1#a(Aa`6W1&f)!^C*Aj*((hE(rFDVC5>TSSyCVLg4{@st;?1%gco&;uoRl_~WEbH%4}6BoWG?b8|#D3WYbpxGj1@5j82>>=luX"
    "!wtBiiqcBX)GY;v{JXJTyoS~eqiOp`-&LGU^uyBdDarw|6^He9I2|sBt-Mkt0~P`nMofPSdJ00cr1u!xhs|DYr(Wg#FjtX_QrV`P"
    "eT(NRe1?NR6u>+qS4`aMzjdq%^Uy2hK6}#WKVWMgHv27qYYUa*bxG*d8x&+ceGbkaEe)lDj4|2P#uIY#fgXQK=Awd*fgt@hTj}(@"
    "1wd=&M;lZJ%W_Sv$3kJ`NIxW0_y99qIb5+LA#`uHC}=ynXP%CK<q*&OjJAWI{QGlS^J62DW8f+k8*Y6eE+>MYsD3X5E)=O8fA|I{"
    "mD&)4El3e9?w-6EeS1W-^81=k)uJYRB$4uO&DBUzNZDL?Dp-LAp&4bO<3%2@NmiCPPN_7!CmlA|`PJxQ2_Kex>cS-^g<mdh;#$}p"
    "1_aLd&GXvKbemq3_osrHbt1NoQGT5mw2I3*_tv5;<-K!7-|)Gm&M_fYgWkMQgB33bXnaphi$}woM4LtXSdYZ%UuZ)F%HW3Uf4V%n"
    "&XTwDea&mnflAHkc-l(uUZG4YhCeq!9^EN)6O!OefackWwI0^DgWMpDO79i9|NS~6{!EzGYTPkxW}YOKra){0ehkcW1||>{7RO##"
    "aaARVB0^*bvZ)!%6siZ>+IxT4wl@qR>wmgh1#`pH|0X?w5^tDk>uJQ~^*<e7dA(~K?UaS_#W>Th`h8PEoMzVYVa<HajI%e44urz~"
    "(OcYFxRn|Ih^I9xAY&&z35pR4yb3*85XbH198R$cX&{~t&-rvZ*lMdW$GlCnA)P9rXx*d{z5daO>4Jlq3pj)$v&ENv;JYKrppz@e"
    "j6r;S&$;djUU;3!<CB2)lGR%jzhBC9qEjo>r9WS}KBjRr+Y1a(I8phf=zS|*O`7e+y54UaqjUDByF%}hdmAC75Vo*G#H1lqg$8N^"
    "io#vrclC`NArm(WU3DGHsIkRSxS*=<^Pchg-{Om>49dRE0@ypTZJP+U*et!?R!IgpX%cn^)%BHy_8Xe@f+d-5ZRRKayKK1Dyas@Z"
    "LWVAq38wc{-i6LG^IM&{iCS<sca<=g#Jk-_tFVQ-vm#+li2OFY-G?=wu00C2+V9NLj-gnlFshBY{IHm_qW_Bf^-xkp4zVd_3Op1+"
    "QpLV15`r7nMD^!Am0gq8OwK~)njF%m&nRPQvslF=@fZM!j%CM8M8DhiOy5&aP*B=JP+m#VHyn6BEO{q;OcXWG6ogP*jzK$TiTLWY"
    "&zbNnZcE2+^}&FF(Z3soA&yyx5<<ZZvU3ypo>pA>(FVmk2c6C$+^0=+^z5fgP8gh<jD2RIv7pn_3-wrQ&`mAE5Q>%vmMIrz`(fKY"
    "p^qsnbF!zt9yS>jm!#1uGJ5FwB@B{tX~;?zJXLjIb+!=9#PVVf{`(FezUI^8rMJYe1Yii80L7RpgpOdoja2uGUC94*h6@Ix_L)Yw"
    "r5C$Ir*Jv67<Bq~iAAmXTy5AO62a|QVyZ|1-`dthBa?E5N52uXvzWDYyD${`wtDd!5%?siUKO@!(^A%dUGv()g4}937Lvy>X<7z~"
    "ysOnRJ3P4<627A4tI>HWwz!$K#S92X%S_~8vUGn~^R}>LG(M0QYN$|l%EHK{RNPtgM+fFLf*_Of*3v=_i3o@H_z+0KN6>CXkoSH_"
    "uHl}cVMlv`K`m;HiNC5r2h<5!B8qRV!;D)nfPRvuzuB-yErJT#ZDP^-?XX|}kjebR%w0OeeCSX#roe;Pi;xP9Lq`9%af49;y=P*L"
    "Ko;))w+JhCp_@V$=FMxrk8E*qbGreE(u_}q&Gyvig)i&lwLd9~VI>4Fy=|o|z3JGJxTTKGbk!_z=4q{e@8@T{?`e8)t{g@94?pwB"
    "0EnL!3MJxfyTM$y5PuGMNw#DBG_YV`j8rkJ8sSwT?{r2_N6@PSS=jU;;a+!6u2%DL=@mrLt4`kK&9#czM(k;O1?o|^HXunyiz{EO"
    "ra-k%WJCYWDB$Gx)<QgN9<f8CX!~Uhl3ti+#3l(c(?o)9v&Ax5QHHsPhGbNYMdU#nCH98wRa3<4rcrn``pub4G&B$x|KUjHtJa%~"
    "C?`U7+@vbvxK6vp7!;7{Garh8dTo?g7p}y8R_v=>ZH$FXoyU2c!Qa<>gdkC^)sFkZ|9$;wS`hNLr>GZe6iriXo_fOx3*|M@DlAml"
    "1dDcc`dea{g?XcinB!1%9ntm=<^u@q-hL^77hxH-C#83xK!YPyWGZqw<z68;U+Lm-*+u1YOZi&gk@{amhk)@#^I^?$h2}Cv3r3!t"
    "Qng2i4G7aFa?|?v^o8{g5^ZujXMq^)-z{h-j?C`qcqxiqy=U2AE>J7E94FZxkhT;~m^=RAjmihDRpyd~BAi#E72fWOo(zO)6DrV!"
    "qq$zO9{sRpxmK5`VJNdFo@OjWEs<VKjJb!Zw627MPR(FKQ_*S#*S$!H_CCIJ4wqB&K7t*?6P0NcvbB8BLSqE%$;?nM^kCb<>^%y{"
    "=>+JND^t^PuI+$Do1*#<1X2v6m#fe)J#jqOGjtu)Jl`XExF*&K<8MkMjoLw({U5%%Z7NrGG-0qc6imh4s-8xRWwQ3(%xDQ&UuL4+"
    ">ci)FusRGko#u)~6{e0NuF)!rqnHINI41+F#IDThH-UaC4s2udwxVf5oz)=9`<|Ka6eIl7pYCh4#_>f$(l>FO@rtq(t4HEC7danc"
    "^+dce9X*1;<wVAQb+z_o&FhKg5{)E!w`OA6XeZXy3ZgD2MNcYgN(F1BG*cb@DEpLQz_mC*DnRPrS?<g6x?T{l16sVQLI({C1^ZQ{"
    "AFSqT>QuTnJ&#UDZi+T{;U7dT&=^$K6EsYIYS|A<-cC<<e0l|9*XjS)qwS@vDN?6hQgmbol6u?;D{P9UpphBtnW^wA>DSuSGTn!5"
    "n=Y-@IdOQ*3(l4?1}=GP=+H*~%pyFSu^>%AJCV<oCM8)!EErbz@c8BRYTJi3-Ie1*Q~gVpz3#IDXV{LMTjrsp+06l4lbXG@o&r4%"
    "?(JARVZ0>Z<R%w=f>c;YtRqTk`gjLQ;o1_>`<9N-T(4jaopGT&1Xy-ExAp}#vvCimdZMg~adhH3cQUR+l_Y5Uy_Yt^E#ZU&24%>q"
    "F{j1YPZz$;;zXjtNKAS9!VTJ1Mo@4ay%MERIu#gP)+z6)zl$n}<33FTi9N)&Ys+=+Lk!)E`tviU#Vz3^lkrVnA)n|X;3o{ZDYchr"
    "b@B$X81Ii6Zz7>ZC()rrf2ojq=~k~5OJRM?Dz=bF)%vdUw5W)~C2)UK6cyRXrlE5@-i=FzC+-CX>MT5_j+NGN)-oNZR2AbSD~t5%"
    "?{@8X98QCnH&cperR$gjr>xy}>pg#1Gu|3K2C?*u3*}Eyr^IZd7rLiejP8@X(`ksGn@$1G?RC^<vuHtSs}JNSlWiZ?yrY4~(G&cG"
    ")a;@$)EY6|&Rode7$zv^wMH*Wqa~zqK`me}u)b;1D>~?-yf-uI=s!45J1AN%Z@rx~`di1Kc~wktTvt0Y1+55{lPhs;pyo4mq`qxu"
    "j^6F005{%MZ?$H=7g9<P7zgke4p?a@W1Gf&DQ2?>tSb{F<_Z*`gph^7EXF4ZWn<(un4tTx=2d1@P7sfwFMb-#lwBW_DT6i{gH}$^"
    "w?*(m$7CxmV!)<gfo>N#ql>#MkJXSrtodUKYZPy}7(fSkW41`Wp@}Bq1mF7bPJse}ZWK1Uquk8O0vRE$f>#s0^Yg>Lz0Ns0bl$M0"
    "M|ZQGei@wOMSq*J4b*c~hN=<<FM6fcGnE}{AGHXKU}qe%(&GD?Po;``qvB4-U5{3%6058<n2R)u>6Oay!7{>~LsUn6riuv)$z%|f"
    "p0pV;<9G8V#SnkT92`EH$7@xeP6>5kFX#1OcIlU4*mG|>a9h--86hd<2xb2fRaWru%yqPPRnK@Q=9r=tKRT*8qOtUpmE$0|y$*rC"
    "q|z5M%i3pxcOtSytBB5Sl<Ku{xR=L=<h`3Q{!GkOG2A28ObdtJ)bu)|E=)R%1wgk}^9j!dMID-~1x-D#xt83{9mQ}#QRD+9N>`Q&"
    "vRI*h2w3uDU}A$hQ)UdiHZWkrSr$S(49Z`Ix&qerSVN%eiO1}RHLpQXMLSU6(+S5ccpx?wuHqb~T+%RK+NlVcx1o4MpO6VlDNU~#"
    "soc5xF<E^NeLur*NUuavSr+2erwrUUQ(n|dc7r(*BoY*R#kputI3sccXeE4Dowl;RxPa`3@9E1`yU2>S0VAXtS{ykKTJePm6ob6-"
    "iU8`yJ5eumj-X#_M|Z{}8L`Lag~`yKKb)0t`>mW3_UUFs`P=Dq3S#LAm9Mw(e1dW!c7j|06Kw^H)7IRC6Np?Xaf)!P{GPJYQ*zMe"
    "zy$!suWXLAR>dx1Wkll)NO<?P%f=W@+mw6zZgGZ8bn&C;qM*pI+53{WViCAGrj>0)rq%HqZis2uq%8?Lqvu%hkJGxaAr@RJTzsS2"
    "<!kQ>gT3wY;ix>-1F?0ierQ{vp|}`5c5%4*=09ze+<}U#cY5%J7uq($5+>76ItfzXHiUxSD_!}KsSo&I@ildLg2GE1-Qa{ml~a&5"
    "337B=5GUeB+Pw(jA<icnHDW=MpAM-00rux-KAFC&c>0_5NXW4(<!e_)kG_z{dw7h~CLV+?QF20Dr1-eePeQeD>f0Ix-|}J23&2dx"
    "r`9RjCW5e>Uax2`!-|aY7jYN;cY`Q|bwWYtL~Z0~tU_Kg0uDOM$~PaL^YO#=5ffe9(Y2Kv=AztMi$_#s_cPWFmUi_z=xNh&)H@l|"
    "2tOz=a_fv565?p@r!(K%NpqZG&Lo@Yc~IPQOj_$-=|V&;30hOwV3^*)8tT|ea&(-A6N5Ld<I#L*K*^7N7viq|+)geUKzu?#`Gh_?"
    "noj&TJsd|wT&xwOgvv^yX|lCBG6v59;o1*tKALk$grlT#96E3e6NuZwIlJgj<b^6mM*7KwbM0K#z*x7Uqb(iA!90nm6wbpR*1RrC"
    "kqU9fS;jTof)r+(p_jR$GLf{M^(-wHMMRtFkvNy=Nx@FsS%hshWttB*g7F&v;&Y%dh|TARAG$3Ej8GCsszLRmiLskLPcxt5$m+Q("
    "rZMuU-#>9(C0<KMtH8O=ycuE$G*W*^^ZCb;2&qS^--4mEgQAwNjh#Ct#i1b##%xtEcxKc1jUz@%dM%i4#I==$O4fmWU-Q~}lzgV`"
    "Nq>V56~+g}wwDsbF3f4sO(<qLu?)c%&jgG20VA@h`NRAKHa~PB<=YY)Qus69lZC#bcAfV77QJfi7#VmG!y;@aw5Fhm_qJE+kBAXC"
    "vj(<dp%F{pH$i!(a0T2wmNaLmS{O>fl4ek^H>v$BN<JasO94!XQgmY!xAa(ESYQ+dbLFq4{A)klx)gg=HWg`4F48GQ1J#O3&ee{u"
    "p+KNh91(ozT_5ihQ7sOl3OXV<%za~~9-O|!mo+bwbK;3dh$MuWb(piDKL*;&2K2C|Wq8zZ;U`8j$$|<wtteBpGr>EHi!>^|ulWSb"
    "#Gi$40k@_?!p;k&c8V)B7zx58g9hwC4BC5~qGMb`r>T^#J)Q8sM%CDTSn|#S_<)%7ha+A5)l#?kB~nyQtJoG5(rnr&*kY==@ZttW"
    ")|&8M$#@6ONeNOQmRn}!6@lLLp1w-B!@}Z6hb$C<8oEiT7H%VC5_i?J&w6AxxT6u17iZsoWGhsk{mM8HXB~b8q7OSggxf!9(z!}+"
    "0^w9*lq8Q70vW?GA|U*SpEL4Bg1p6e9VUtjR=)|AzlMx;`|_OeDx23MAR&O@tazU}rkDB?3ma@QEKwM}#ITIJyCRrN$0H-yONb8*"
    "2ZZ$Bm%LFOG@Fm0Bf)Ps+LSyRTdCn+kDEoPr+ag8u=N%?1Vu;*zc|4lr5W(b{IF)cver15q8NvRvTy=oqzDnzmfWN{L{v8y0fad4"
    "CehZ~Qy?}8%VwQ+`oz-iANrI3GW%uj15PBK>_t^qjC6RE7P)K*%0-I8iv3C$f+X5Q_~0%q*~<(lt*ovs-s*=SlCD5!t@%jx7QDxU"
    "(?K44O=id&?5~-PzocbI7)w>1XZ<TNrHGM6rPeocO7p|Eea2qA!G)RfUJ;37z_JZ7R<Yhb8G;nm6y?k`w+ozX{W=~mPrXlUo3U5q"
    "I&^A(d|2}i)Y?j&2_+iQauexs?Q|3?zH|ixfmeL1aaF`#Ab^Exqe*bq#c9GFZ{#n}c~zSzFZwfXrRXDGl{QFTheHXKegL*n48rbA"
    "rsS?|6%<Hm<sy)`8ojvxNFT~x<>@O7I+fniCz`j<TRYapIff2B>4onpfF(8{=;egCsKn?m`<xhfFcv#>P>!AShc$1K_vp3KSC7qy"
    "LYi=tsE;5;m?R`X@=%~_ts|2vP#7_|^LpZWlD~FRUU9#l&uaeaI#KkbwIVJ0gK%M8$XY1mBz-1d!`L_u7-VzxDXkFZMqU!vKUV{@"
    "Roc(>!<zNVvDcEg$`?W?IbrD6nHko1(&JFOWJsaS+9&F|!jB3f2Gy}nZbu^zcfRos+UhO8$0RI6i#$3!S@gW)3Zx7Z@s83twe*F>"
    "uDH|@MD<N+&|3wDjCdTc-&C5Q-`8|k(%(P<j=6f$F;M~&CJMOaMSUnmn1CP*YVc`e#bIf-*5~!K8_qLrBvPZa{b9}9uAU3S^vs|*"
    "igyXwb?%<g<`Qh}`mGr14P3>GiYaSqt7;<)$CNIRCw7Jez<VmIj~EiR1%e}AL@e|}SM-44yK|!!626K;vEp`9!HbJDrvy&%Q}K<q"
    "&bn;GY)8acWK7!>;~$-BiaqaZh96o(<%GM5$ely*SrJkj&?k)8QQFlDp-`l~k;GZjHspE4YPSx9K1s;n5dUG#_>>(G#KP>iS0$Dr"
    "{#`1%k_EbglhVSvk*vxCZgC?8l{5#V1GFp%c6e=^ccle7T(0U9<QRB~zSdTTQy38ep&dFPzh=;eA3AvgO4{#w5yH58Q%%bFAB*}$"
    "v5fZwSf86*?J0&HY0hXuvx5D29O!r24lA%Uw40M2G1H2^Q8g8GhaCnk&$WL#dZy8j56}5D(H;}u%4~*Gr*%%B$|L3_I!p(a8OCva"
    "reK1E%sz4!xc&5@=w(Kx+D&o#LkncR=9ONN#N*&iUeJ##uApntbil2?UGRz)$~8b4MbWtueZtElcDGj$ebQbMi2ndC>SGtB=vSeS"
    "NK~T_M6<E7yKT&lc^p}~*bML?EBF`6j<%br2tI_DC*>qc&*9y2Ol#hPt60E!&MeT^lox1SwPue2&!=BMuZp~tr~QZ#XC%@Eyu{29"
    "{uMa43>$w4Bu^!>#M9H7zr>M8pUP3;W1Q{%hcIcrbvZPuzBD1G9~)X9k;&&PaNWHfZAZ|vTyikV|6Ix~y%*moxu9WyW+>C;HY;&s"
    "w5m^#-hKSik0q?B%p2hVApkUvD3Rjjn67KzugE=F6Asev5u@V#L>RSTGZ35gSWE}~hrVC<ux7cVK)If@r7Vw+r?azYi#x@^$e&$z"
    "K?uGyl6GVKl`d^+9(mv$YPDN9SMGhyJE^C%C>Rpx)LQ~;4P7*>J1_}%!woK|J6E>CdT71bKAbjPiQ3<G>qsi3cvKudtogX5pgNk5"
    "CA#>5RgBMIMzOnWQVU5$e;lbKaW1qumynQ4Iu{+!7?Xs0Gm>a-d|2~N>Lq<A`u-V&EkacZWfmo!JQTC`63W!$?<|n7ztA34s6tk="
    "ou;Ewx$xWLLw`jw*OwvbIsIvyGtfzqS9+lqUWpl$C;TpOMJH{`OaY7W_2|Svm+}a=brIdtI?9JNAFaYb(o<lLSj@SA$2wJSaZ6_M"
    "s-sH_28Mqrz8!kg+){}q<r-R~KWwi3&Jsjro}TS0@a)iFR}?IMyQI8Ag@)RUIGu7@-OL3dT(wGdzIR%fw|9b`Rlrtcn8CgIVa@Ar"
    "S93Y`tac~d7-#i3c_e*DVY`O-D$<fPHq8~!h;G-O7PL>Gi>hx~XAkwunvY@_sv17WFQMxWYYK8=G_5dVHjqGB3k4>F1gzKcVr0s_"
    "0^Kh5WD$iBLC)AdYiCmELg}!_cir1$pIHHM;EIg95Vw9dGkLI`vrN`1Vhtf_FZj242i8voN{nq02^~RCsMjVA9z<>4r)qF!c#;;9"
    ">GaV1hO-Kwy9`Cd)fu{)%2$e)jCW_c1{v+Nf!az1ie*8eRP>vz_l6hX#=Q*{>n+-M`ZF*e_a=8mL0Z`MLiRpjrf63`5Fv2qixkF8"
    "p0@Vx!V-^){k=?@?trM!vA9C~qbD<rPdJS${3+@y8hVUaz^p}KGmFxqnmF*B8NG83n897#Z`BXadBHOxWv4>|riSn%LWE(LFtRx}"
    "hX`QKB5-Lj%@9=CP2V4D!^Tyt$eq{U{IZ9xI$m+yjp}ncZG;x!(_8<46}>grHvEpc$mO%5cWVQ=-^%o%HkKlYvIMfx?**%L&3bK$"
    "Yzn<vIxwl-r@)+@A|{h>d_ZN}I%Y!uA8s!GKbA<OP0Cpd@n0{V=*ovRZ~c+(cCOEIZe3wtuNFTvHicK%->dlGF;zs8E^~3GI3qy2"
    "H^Zwv=vU|i>ERD+-b}=BJlJ{XEaqYI@{FO2aOijl*P)uf75Vk6pzfse8L&Rof)>*tM(>Q!?fURJAJLRVEt1JB)LpfCgd1nFLg#`u"
    "%7!*XeKn5>(kfy1jR6T0>q@~mXNqMG;mQ2o0+)W~V3o{85yF|h9obFH?Ujyp+{uZlK)l3Xr#=~Uw4g(zRBW&d;_@}>m4v=@YF^iL"
    "cY3JkO*yik!dl?g#DXbYM&U?#kalR{Cxkv1BI>mZuZ@f>fqs!7EQLAh%I_RJUDs0z^SDTVDDNltmtR~o$0a(Big8KO-ZIPyC>t|l"
    "8tVc&7||-O-;Lmc;+D>&IF&eaIL#cPeLx28xt>0{Iff#%_`xdLTY4Le2zKP5k&6~~O^`KH@W|3BKyz$(nDXZC%!yeD{l*V#mfJLL"
    "@EOOSRfwRZv5en3tsdbRwDJp#*mHBBzh>@Pc(T^P<EIKXSf?!B-`Bi7iJq=G1_$u=5(KL_5M=RsWdLsQ#0U%?a!ZF9`3D|afL>Gq"
    "eXg*`DXpzwKCGFqh)qlh`w4LX6t@D>ew&(WaXLUAuSJSWW@SndsZ_MH6~$|h$mcN`P7`c?U-K#c=sW$%eq8r37S*vt!h1}xB6A-~"
    "=OdV7#yM6%{iXM<%+DaEHlNVD+wDWR)Z4oX>=a5ND71>xp~98ONZ5OJiVMua4<P{NKejfu@p|QqWF*?I!emffVdle{Pq@T#%veZ~"
    "4jESz;#c~lBvv86)WlN64@}m_#wR)p)zyXB2MV*=?pvg|ASv*UX6}rzvsIk)bjjecy%Zl6jjg!6Bjpw#l~TN-9an^Q>*4VR=b1$y"
    "wk`V4DT?;)9FVR(XFiV2Wcd$2^L!7d#34QnkwS4jQs`gUOVc=5g)BQERODF*p|8v}61UjhD#{11vy}x;w+SEK%^fTjc0-qgyN@nJ"
    "UzydWl!_p(ayVU-nL$GoNtCOVr?DZTLld0}df=2-<67^;*wyQP#Li;ICU&uhdQ=F}P}AQGzBCjFC??v@M^N4#QU92cShuL|1{!Sy"
    "Fh4L1SFba0rK-X-vl7EV+t+uG1T7WxS%3>W6w;KAoxzZ}C+y!b*h4lufKJdg{jlcM<?x8GQ~J>sjtFn;p+m-mOA{{?Pc7Z9if<Wq"
    "*Tw5^i>YFqa%?@d4de13TDX&cF5$gStUCi1sEW46%usKL`5DDaBpgsj!VQtu0-a@1t?Y0lu>~n7f38cI(|lj^&P>tibZmNuxDe_s"
    "V=w}XGnwmRkNU$`MshYe!<90(SXCGh`^fDXAmlE1#PMOxTMiasbo8ZS1hxteWXm~)Q_R7;s2zHvV@#zw?b2g!EizU-E;54I+EC)@"
    "B;^llKHsxg)Gj>3M1oVyGKq3Dg~1RU=7f4MgayWM%ca;9yJ7>(hFTClFMJN+?D&pH{5420nUBkwf|9f?6)iplb{K<KHWC#O1`;9*"
    "c(jY2X<Y|L*`V7<QxicWg%4{!nhMH9^@u8b_?aiJ>$BIR9T>L|8qUK4-2x*+ge~sKX!oLYXB5CijMu{b4?%Rs_m{z|S;ug_&vrrg"
    "uEs@iM<=|BtPnhr2bv}wbt8^0qJ`Y0PTZ(z<f)OsCdQ^6y3wOi%olTfT{GO?<ccQ)%s4RJ?K*w|+Dale;%SP8C^OJt3FSf6M=&+6"
    "dE}Q7EYL%a(4v|3!<x7IQBW##S3LDZ1IbjXOs(OBMZ@>G|IY!nqORn~oSm@J^jrpvf$EKrf&Tl4HJ?6V{Ke}ri7FnQ7X?faK~t2l"
    "xrT^4Z^b#Ea?t*!aG<Zq99<Ct@t9)hgA{=;`(epjba0IG)-O5py7yIX5va*6;RSv0HNn#0h~4qJrr$|hO}`4(61Hj^ponAw{V@2?"
    ")In2Y{ICVC^LC*@f%-N5`vOJPCP=1pEsC@&8Vb9KS4YPPp|I=X@)!NcEcDc&-HHXtd|=bz_A_fqLE_33y}p0aFFf^qD&4;#@4Gg-"
    "*1DkC!~%0UFscB8A`|MEUFlTeiI&n2Yu<6q8nQ<%AY{=Kr>)l+X6U<5h0Z?W4ACq44JXz!6xYgZRrs;$%%fG9xG_;SOCQ#J*mR|6"
    "g&gSY3a2yMi3{2mOHwcq3ioDt#X6H|eo)BQZ`0^cKoBSwT<I_~RZe|fGuqpt5TH^DcS`u^z|ZDK^NzkAH*7xJU4b@mL!y*onZ9--"
    "=uObsr1&ztL2asjSo4aG2$4pv?orwh64_0_S`1yNTLiWz{poZ?DNSY3`$<b=O~6>>=`H5!R(xyc_dRqctrUD5eIp-6|Mf%9RT7%V"
    "$RZ-QE37$jtru1^xJ-qkkLRfj`o6XK@wlnflNMG@L`U|0#<BB#$t%T(o?oE6VTTo?QH0Lk1(-wB2UvSxafEPU773bzXonN2f(};X"
    "IUy9~=a<USHUC4mNcdV%t&6o~lWihy>p(N;Hx|<$3gLpfR^d=NUc5!WkEpE0I+DpQ{z*t5_Q0czfmx|`HEm?tO#lmDlqEqng5EgW"
    "1S<s5F?~>MC%B@*+)$b9IsiKL8<&Cke!gP*OUybQmFI-JPAJ_8<T5$0uh>`8S@VN|Jb{}^liLv2GXrBli*b9=Tku#rzC)P!n1DJ8"
    "x1KeZc8EeF*1MD=VnLTO;~?gG9|~nlAC40hCW`tVn~lJ{ogv1&-@2wBQ#|~dDrTqpV@9mP86JDusR+gmZqQpE2*@21?-J=~8>KQ{"
    "UyTykLVB8*|KT|wiC?8b+KcJiZn+2XjDldu6oGMiYC&94PG6gzQfCNHgPr`5{!*eHLq8N+LTB;A_xzD<6CJF?OVn=Z!I%uI=Dn4-"
    "^<wA1(8vjIaAhjI-RABEg?UugJSyxW`y%}B&w0nt7*L9aFd|^NagP^isEWPVVB+(WQ2Q8*C?+t*<pF}UI>Qya5n|hDvHK6@$8;rr"
    "O*+DH=zRG|u|NkMK}=CaeK%*(77I)>4sI`zKi}JXY9b2UVwDyul52XW80kvi9+zll>K(m;8L65o{(@p$5x5=n1GdzWrN+=P?P*Rs"
    "wHMxtk?Axs6A_Si`j*0-<Hrz`c-uRo*5bM1_&5w%u$}`xrm0Ye8(uov-&`0ZlxWw`=N5OH+DPy7@mlj5E20wyHP)U`_Wa}b5>T4y"
    "OjOTBdz`o^9o?cBLNM%}rhl;fF$QCVsMs&>_VK(x+C$+M)sun7)6Q$>mLtNOOOV-=in#`|GPo$v<g#6+@k;L@i{s9$os3(<hcz#)"
    "VVoy_=&nMu@zFRirO*Xtd~_IlbIphC5Dj#K9SL1KuAG$vf>OsYeJ`vBDCw7p?jDyM@7;?^KX>{u(+f{?)za%wn%CR+M<u4yA1&&K"
    "A<5VV+c!tE5gSms!~4}Ta%m}H9sgkqJe{GXoOY)2<<(qo4V-1n@lvGI$umftr*C3d-V#oAGLbq(tZrbQ7WI-4i{FJFVR7de^?>=k"
    "-Cr-{s!`KYkq;9R%OMxiYUqc;xUzHJ6Vp6UN2Vo<!w?wQqE*(c!PvjY6K21sl5DSfBXdI>$>5e)hCT+S9%x^*>55>FK^wPjs@%xN"
    "Be6Q@mtC~S1f=;8QGZugMa1<{hRz7aqt2pCk!#<3ap8-4dGmz{I!H^S+Jg$5A|SoI+>8dUADA-~ZnEV)tY?46H<Z4@@mGxv`_Oz)"
    "fbW7K`nM)N77GRD&@ljA!pfpP`r-zYC77n-f+FO*-#g9ZkMfOnN9Bgc(~}={W>DGMB7zJ<z?`SvenbEj(!_Q50vX_8g(*#?aH#F}"
    "eu<I8&x~F0jxbOcL;1T`%;>nqW{7fbK?O(GNTQEafLBapJ3APw4$8IoJ}f@?hc%z3zQAaYJuNa#8LQD$y|tOfO3iyFa_3R|JDMQ7"
    "GJ|K#9Mt>WKy#WABrD4J;oUx6?X_Yrj+R4Eikygkq~h`plbdBxQHEb=qE$x28f&3^&C>mt4{ZFCEdsW$OBVB&aJx{Vwk@;h#Scqe"
    "Y^>F=0LIcyp<T=oK~hsJ<INF0RO&SjEqA?HgBHw(HSfrz(xvcn{K9JPpo%a-AmS;m>qA)Ox*;66hW;p|yaqi?!VDX2`=AA&p~f#~"
    "oAMnF<Dw-<4S)GX?}fNHRY+836*^Kn-Qk|g3}p!=zvJIyg(Vm;n~q9Mi<z+{u@ENg6jmcQTx)(<^Ram%X%%zE`Ub-DzEt{Twb7=S"
    "lVkpS<9J=dHZ*vSIpP_XRjMPZ2`ma~xOX)x()m-y$>e$9zD_HzPr<{TD*ka@5c*VxMCn3AE+fx~(-aEYq3I!y(7Iy6a_>~CJS7=V"
    "taDUm5guh^!>rYhsZ<eKLR{QZAu|F=sW%J3FDQ#LCGACNxItB7YG2lTTC*}E$uPz;QDv-ai7@Jqcf%^|XG``*o>uU{BMtACfLhPk"
    "eCxNcM_x@I<sAi=hhZo@XMcp-xIqx>ZYwaeYPX9+@TpP@Jb19gC9)qyN)^%dl=?X>nla_ZLhZx0eHe!J56&g$%2!l$6+4m06On8}"
    "@DbB4qc9WB)aw>D)rkpPpP&{cA&Z4{)HkmC!;<x9!ra9pLz7XP69GKdUpq5KZqP^;j}i~qo(l8cr|M3<Q~ZBB?A)`GR{U^3R3Bt<"
    "F5!8zBZ|}L5SZmn7rq;!<ePpDo=@nH_|@~A%q;8hl)aOF$7+~lQ!af6GxY)s#hP*q>P>4MqcRNpM5wY(pdM9xLNP<<#3BQonE$Ft"
    "-&nu^HCVmsAwo`k{jg@ZT3CSrZo25RBA(gKSWlaka(fVd*;okK?n+}8)8gu7W}z*L9uLcaOm*!`O|L#zQd+atV=5f9Q@w57i&1bJ"
    "GV-9a-A8TRIuVN+V*dhFWj15PN)VwyFiI#S&HCXvZ`S}9QV9?(l12u{%RBVr1s}jx4apzD8SS7saX1%<g)}NZ8}Th1)ZEDHL*43{"
    "li|c?iaS?~Xt@qybUmjQX1+F&P3#b+7uMEpMK7JIy7Qs34Unl8baY&HzOQ*@{H)k_L_LW+Aw&CrsS39S*CHa0{wLG(G_h_KV~T}Q"
    "Us^J_d~4#dO?l765AXJ;ZNOrtpyz}HV;HDRsxOQkVbrnFmYka?FhQY$SE$BvH-$g$(3b8-=#q-w`<meb#S$6jMYdh+ux3FNeWT9&"
    "Ql?8uJ6SlJKJ7k}3A1~HZ<XQ`j<lvy?%kMHzOVUQEqI0ZQkdIDF+E_)HyPD(9`T2O_~Q`e7QwI$(IqMY6$>RHhc-mW79Aqnwh!<2"
    "NuDR9+5=2_RUj}5YRF@?h9xVk*^>i2I=NZ@kbZ%r{c4j4k<Lq>b|R7SKDV8&>z4yTc|!auLnNtaw-_|*H=ZLM%?pbruXsA<5u~zE"
    "%ba7kA>tPmfX!)6cqbb*U+Z1%5(gL(8m-(PwG?bqj!w;>LRG;>_;2G+tykF)KJcM*beBpOukMlC*bi~2+<P15<<K~&oVSFcXw9}+"
    "%2EvXOhN6ss0DTg2Nln&6CGFr$VD4Uzm#WCarF;t{?z9LRk=sgDWIRV(_vW+>YiFf-%CVhX0RT8bE#rFmf}JOn}(*e&zPcK$@`l1"
    "YQ+ny#?z`ti0jkfK^Jm~ehYQGu`<?y81R~*oQZbd<k!;aL7droWx7ct_S>2m(O$hn22xQ1Tp1}xbm|1|wiC`=DDo>DOc2CznGs6>"
    "DCD|nm<mnDS+Bf@_+D>S*1F2M-YI7zL__PcaGw6S4O^$7!>1zwsQr=_b3>)9gt#5RTUa&f5Fu~0tB&*gn$OjE#MUcNx6*6GCW8Lw"
    "nQ|*tG)D=?Pk2RFs4YDW&PzjNCyQb5kTgac8rSXnnm>d!QH{_Z%10O7ZU#f_2s#)W_M|v35az~C=n9BHDFL{n8jX>9Qfd>xUm_9j"
    "Yd&Pf;~XAN$MDXkVQ=Y>rAB5{7$nCD{$vW!2;Xh9TN8X&xWM39eA(*@j6!AL!<zNlVdZ=bMcoiCSlYdo!3jT+^a@5<8UJ!c{-HPA"
    "Pye}IKs8&(W^<EK0TgY2c+RIaELeTea?Xm4HpC>RVsHZ)b=d`9&4$^2CW2{|ni&2rC%z_PtIdpYF0FX)#GInut$m^)6pqsPBqHpM"
    "jVU93Q>jIP$aZ{kgmjNtt<kCp6Gl4(auLB54ilen?`z&Tlxf^7$Hra9Q1rFY>r`DwSXiXXc>#r<e8(e%5>wYN($F;X&7WB`yh)Pl"
    "?`uA6C5pEkMKw1)+OnRlq+dxN63h<hIMrRJJ>)`$MlFop(5LOIW-5tuDLK3@dFK|;;4bz{i8tF@=FgVdaeGF?5+SLGI1S5uM-rD%"
    ")UUXID8_X9B2gX3byOHpRC5n{L##g8d-+?3_*v8AdCpLvL#d^;jWsO}<ZIwHAGkTlR0xnxWF8;+H-iIo(n+gABMZ5t{9(=Tgs#e1"
    ">2aQC_DTnO8cZgqp#D<y6oYq;j-fN^XxgIr%rV<dkES?Zk92RX_aV=0ABQ{T7QsBs%|w}K0t=Mv@rJj`Y9jk*=0fmcRuDz%Sc{D|"
    "0yYGjyLOQrrOW<d4?H!G3g|nUmpLga3@#5a>ea`a!#V6rF!>%jKWQSXm4rxXM4j68Xo)h|t_?Wfmwc3vX^%w7j%Y~_Hhm?f2r#zT"
    "MTUDWidc&g|IJaRgpXz;J}Mq<fWTzS7IgIQYhHc)h{?jAju7Mnsu|k6LNJXXeDNJh^;C_mm@%p|;xdqb6n_0)86cOo5bS#taaV9a"
    "NI}7XOjIZUC|!3S*Ro2qd7I$^+NimQp^u&EQt0%+4Cd^P#=000Ju~kOjoCdcnNn>%=rlWg*16=;7TJm$+K_Mz9Sa?Vp<k2vYrS{D"
    "JCjzYF$IYS1SN!K%!f6v0hbk`g_&z5v9hT%r#yk}{kcv?d5#ty3OhTXtzbp@@+ez)N)U0up^dP|hj;soKZP6<wnAsV-PdWDYQxt}"
    "0A(K2Ej`*=%C%x$0((yT$0`diWn{$EHjT@s=MQMcJ<V*kf(I$Tiy>;8IQ>r-+GH;}CWN^EtVf%@=_^nJ@CM_7t_KgDp@&Q(%)4<a"
    "cAgJ=WXg+cqhOanlvr7%7tf5!eT8ro$PoX|ptHMiy)aa)IW@y~1jv$8&U)`G)0$Ukvrzk0Dnb=#bb*PLg$6S%D-}JC$mP)ytqxUm"
    "7(q3ofGddVXmu`Besp@m`<g%cOTzvc>v?RcXz~Vm0(Eh_DN4O`ohM}i(Kdl+u{Su6k~E`)*x2fQqs94-pxmQZ08186*ZEAIP;JG0"
    "^YMrk6s>0A=A!sRaH?*h24j#doQi-`*)611h5wN#d0+EhZ7|Lh@CIGze_<ovbx4=q^g{BxS`^%oRd>|cLm5Fp+$)vod<G*hUGO)v"
    "?Zdu(SaM3}U2|6U(p%Is7(D7S)lE?XXY#V1U{R<dF()MU3gXaFMNKb^Qy-}xP@DVFQuc<z4IL%=Ov>LP<Zg>j=`aYOit`jw=SG^K"
    "DZdgxCPwfX!)$ymg@+KjdSCNVu0Jz}==Ds@B5N?{xM}MOa~S&X3?Y1pl1B7<?BHJ!{#nAMNdTO=-Nr55hi!Xh8OSR-@*iXXK>xQ9"
    "mws!SfAyMop%Nt=HTAA1(!U5~D2Nsm7ZEgUqh*}$N-y>BK1FH$XMS(S&i=V;Dr`Ci-PdN=G>)V%j@!W!Vi*EC7=CZ8CMPO2$HP9x"
    "jf|fSwMG{KS@m6NjNY@SfKVM{)k1A}qxKzZSHNTzU4PN^#v{Gw4OI-G=c?BrFi#&)B|mzu1ZKsE`@@<~pL<{^f}({Uh}n*&@HE-G"
    "`t&gB?RF9Q({aM8OQ9@a;*yb!(m@byXeDri_>S8AQ&i;60GlU$PLV#nKJivd{}qZdLK$guu^?hLwmFlAc``4nt)s|cir6RPVM-GA"
    "!<tuvtCl~=UGRi{iaoj7@Er%r*p&3NVe~s_UCx=%?<fjAqZ_C5hJ>LoqyXM~kr<h!hlmNy#N&}T76bL2yAlC;eNo$lM)y#H+FZk}"
    "riT<85%o;kQ%N*!P1~%;yY~IY8-JK5bdmDUxDA3rc4Ryu#E*iQptdA-a>DhP_14Iyo!jG0ays=GSQ7OYt&aP{lK#pkIp%-qzf+pG"
    "6K_({J)!i*R8JGHqcp5!E2h8WQ2J*nBr}xsmvBc#_~rwz_J7IMQ5qmx0vSd4w1cK%#2$=;y1i*5vfMJdq*GzcI1{X{VQWAiTt$xm"
    "gPkZdHc!2lc_nnF_mKn8|85+c!=Ox|+&3FW1$E@CK)__;(n;yG(S)|qFJ4DMvb|?NpDu!m3N`eqHb*mIDhh1cl0pu_D}C>PRcmJ;"
    "MVVdTpT`#2MCf!R(;Qyvm0R)mHJ{pP;I6Z>hayg;Su6_Im`;tKa!|m~NuNs_R%lgWf;}#*wnnT@99z4t>Tve_VcV8#SSaREA@frS"
    "!st7Bn@h0h9ndo2y%3X9Uz|c0jqxEg)|PVP>c*opsdsMI`jByb{c+<)L9%{OXb{f&Q8O-Flm7ER3H0PGDkvu6wn9VnI?l`kxUB8H"
    "*T@W^V=(1VSjt=&2}Mt=v0||C!<yj=_9`mjdn)Rc*BCUcI6S<(5}g&?6(7j>XN00X*07~s2xts#4+WG>bIW8T_`{k%ir7Ly$F45Z"
    "3@5N!23}r)i$M|WZfBk7t8A`cIKa5*riI0&{<@<Muux}vU(;T3nOZSo5=GgKa=cpsA=!<>9~PL$Aw#jOzzT_&Q3SWlvUZ$;;8keA"
    "i>CPZ7=7n$lu4NC!81zP2GvwoL;(}W$vYgVMvn+bV>lobHX@TQ$5O{nTm#UCX`%TK@77-z(yEnCAfYe7bQ~+Sy}N?e5U6mgV$EBL"
    "+B7DLby=j|;xvWMVsZ3_Hu}4%tk%56Y#3%fIT!O0WtU!^_M#IVKr|U~6R%yIrgb{PNN?{JFzxZec|-U@ls|^|Im{g5rMWd1bF?-U"
    "hMD~oWHj_(D<mp|GuhJ$mXLaW+KDaxt9>H|ZF-_KrG2DV@=mQox@smqyW^)_xEQDmKTu*7Rj;M`j0ZBRmwGH0Gi|i`kd+A#9UsOy"
    "lPO7Gq<z>!*8s}UUc#jsOeY(G76od=o*2$j-uG3pDd8%I>0%Sb9T^i@9Mp{w1Y$-ph5i2tUjMrOv(5IO?Z1ux_&-O<?@xqIWN&34"
    "<MG>KFXF#(v>vf|7-0%Vx^`R=v4B@VjcCTCo%N3l5BdDb|IgL_=YRbBza4)@S1N*98*}R*7*rz0ny4fRS703>QtKFx(#OqHX=9_7"
    "t6h}nX*%PZWxg)=uamdQs0=sSs8$6f(1Ck{6&ws0U5%xrpnb#*vl4zdZW5eOZpZgPQ&xHJzgX$t|M$QAkAL~M_P_qS!4Kiye`JsH"
    "&fj_9`4|7oAAa&dIK8b%7T_c_(YWT?OCi7b+=bVn>>jEf3wB)S3H#8uqIO5?Zdv+O_e#H5`>D8!;W+QzuYU1c#tgu4m%?v;_&H^Q"
    "A+%Q3U;XUU^2{Y^3h6gL`$%LV??;p2fkWDXQPDZ$wzQNSCikaee~yO4UW14Xkxt0{bkftA5iJv}I%~gp+fP4MG>J|m#%Yv!MKZ<L"
    "nTX5S+~KcQl>Zdmge?csUt~;r1%^PW(K!(N=(`G}?aymlbJbA@n&ww8+&=DNLJvCr2Sonjho26=Tu%4*Z+`Z<SWH;7)N<_GoHxhF"
    "vNdE71=Z)Nd^%yqYV}S0G}x*Q;-KDcMB9wqD3G=O_G)@v`?(y#-K-vs-qB1D;fokW7c|I+qNKi;_67B%rqGfV-MW$vf=u9R;adH_"
    "BWt?8ul<}3wB|8v{>?jk0Doat<6Js_|JkRrUr$6Q)bAF);>{>P@dkeL!?!@g0FDTq`PC0UpNi46!{wBRh5u;sXG{c}MiE$%xVY#("
    "G{Nf(Hz&#^22um94T1|e94?G){`OCNUAx|42%J&vZ=#@kG?}^L4>|c?{p`g?r9^vlSik@96%oQeOL-^%>W7~lmO+5#S3mnRoqmY-"
    "VIpZ(tB-=-q7Zl=#psaL!fm<hD>3hpEbblkv;>RNMffX*xz?IrtbIEu5l-Tn1)YhjbmY3-i=TO5`lExsxFZs6Ko&swK%`sn9wm3J"
    "y9&-FTswYW`!V#x>`YLWc0aQkv$U205i9<W-3E%-Emd)l3>ROR`=uu6ZfS|lanOAeMO^=O0eD;c>bYf4Ttg|^=OOSaG$bzWEb74e"
    "Q^kr=`G~8wge%ZSJu=d&HAVirRrs@Ogz~p2((BsSDuzRM41<30!#}rN<tX3(=7*oF29F-t8NBb^DQhdq4xy<s(SvNmq9r;q7LBME"
    "`@y7`8G%BYb{SRKMRR1e;TLP)q6>{Y;a5TU-dCU`Zb}K}S3mreUW9t9xBgc@`~y&Y5ICZ46A@hH-VOsaigGE5OScuO5t-L<p*sSR"
    "=aQ%}C)9u!!Y%_rpkHj1M=?%lHX?BRt5;V3@xHUDU<mnF5B+F#AeLULaliW6r=aK0eXje}FI|6p)lL3WtibHoiPd$%{IKor*dq2)"
    "xo@?xVBa0~5S`IyXsGALE_~A!Ezvpp9uoXh5UPmNMEjeE_Sb8cDhp$Czxv^8GK%Neaj^gX!?$M|(Q1>N`PC2KmE-Yf`_;<VH%3^n"
    ";B)`0AHEPlyx~+7G99sk_k9PQn>2F}=%7#4g_h6M`s}t<5p1OT(IKk2i=X(us&at;zV;J|^ul~OqV&J`**kAn^VxsD`QdcEo<;b>"
    "ukc)W{HV@-tv9ax>W3fawvrUk>TjMnSo@dg#hKn5ejHtM<`je9RHhXheZxaE>x)cR3}fUQSz=`q@sL3w#^Tu}D)4>nA5T#yMog|N"
    ">r7SDs$W4zeVKW>D-GV3yN!v~N7Z(m3K*?7rXnL^o>ou+J@Q|y{b*<yW>$ZNL&Gy6DH4BN4S(~qPlAuJRG;|Hj4ONEjrB4oH#)O>"
    "OS5H5zT=#lYZ<(k(gH30jgUPOqxEz8m4C7JwXepX!I@uevUv40sd!TJZGDYtdO}rGN3>gvHVTa~fIEikQl~4QoUZe}6#$Yk+`nd2"
    "-o7uLt_ra}w=(>1;#f#OiUHY@*njoIPaHE`P$LIo++x}t^NfkTdpg3Zid+I$6>kP%{6=3afqzl6&9rj2?vY{r&DXlO{y4{{80~L<"
    "@kc@sYjp2upk!oJ2Z+YRiL!}k;Nt$^$YNL}-J%oTkP+!gUk-1JGiGVJ-Qt$^7i;IILjsd`|C_`S>h&RDATs=Fi^j+K;LKGTsJtLT"
    "3SB!;|Jj-oHEshZ!(eDx&fQGfd-IXrh%^@NVwiZs7Mj5JeeIj56_b$f`%OTJS4>O<EYTzLtDpAQiYNG>m!5z1!&hrkWJ|m3H+U{S"
    "{ZvArB2&LgJn?CLEeY*M`<oydpPOt?E(m9Aw=o%;K;Ser#vm|(Vq+LySUpF-@qW<j9R)9`?Xmhc`WJ0r0z3R-?Yl#Ts1VWq4H&#y"
    "KgmSHq!!YFvDr9c6F0-tHlZ0k#LC=E7!shht%KN_rBz4Cj&=Z8CcPImx?imQXd6{5-H4(AJs-Q#62L&L8m(w{GbM1xWeTPRO7+{t"
    "{C_L^ax6-69Lc++oB}t1#9>C@s*X0=+<nYE%K4*(dFANWU0t~_=--_L%t*j&tdshfZ2!%hClM??cg%e?#L}b0#QrqAjH5J)0xpi7"
    "r5x;4*i>l)@YtDFm>}P5@YJ$qBUA6ZObqRR*oXhOx$PENgHl%>f(-f|g5H)kSEPLq(1c*2a5(_rZ&HNwlyf1HEW`neRPtaI@~OLi"
    "KT_4(PnxjAdHd~HEP6{`X{8r@1X16Bq{s#K=&rXeXD(WaPnH=^+BqU0AZ0D;G~mk~gvHLXVP37#nUE?EgYuDd&vMND(|iYVtHl$="
    "^}V5K#+K;f$=7;ITr%F7M%RI?>Lm^(v-89?_PrUxM@}VpV!{=dQKE?&U0?a7@;N5JbVkuZ$w8XDNq-uxHvK_T?d6fEVaYy?n={2U"
    "Xbw3Vd+aycNaz&tx4Qm+nfou{7nYv4?D^4G*(A|8Bh!jt5aVXsR8j_8JtMJ*l7;-0LhP)34jbTa5o6{Mc>_)<_et?!(eHS&-V?gY"
    "D|QOYfx8_#vEw%#L|U*<2BAY+fXS-K476Y)4JMo3Xb(>{l?qDDLYvSXbKAY&!H1$QZ%*?7hi%&L0VV1L;TgNjrdwE`GaQW(GXmox"
    "Gy^L}R>Om=V8UvW?{i;|R5d~VaUCw!oA&~b&#UVgqrPoqAa7P5ZIAC`U&m98lEY)v8);lOCh(k1>VPjS8o=DX&uG-}1U<_0F=N4^"
    "Zi=_IMlCBgpa5)b#~ZutsmeZF0vwe?>7Z?t=h$G!W#I`+lmoB<z(Lt1ET)YuHmTG&8lW}!K`TpHnB|!JJ-d(GU@VZQfSW2Z;T~9p"
    "2-&?P+B!;Py$cl%c(RLsi;elnV5O!5yffJ^$5S)M-0yueQFU+~skRq|w}{QEt^>lKU3Fl?VM@g{_;={UO5o;tYNpHo{eK>}BkB$("
    "Ht0@yRO1pA19z(etU&z};4@|E6i~w;cwlc39m+C~ViR6H{VscO%x(9+IDvJz0%^<Fi-CP`rDWIOmoc%P575^2%5Hg7QNA?sM1=No"
    "dZX~W1=lLMYHZY1qcW#!@SCZdKH9RLvA5#VSd+u^R63cA)y`lIV=9H(!t~ju-K;g_j>?9d-I1FI8&wzY>RZ|wh(8Sdeyn*lUs}y{"
    "*!HXuyTy<|L<D`F&wOP&D5062v0+e~BIdpZ?T=2ND`H-q%{c7udh9QDHTJ^JzNxcLhpqUZI<@1N`(B4td9tnN!SDK17OR%(*%|gy"
    "TP!f%!>Q|){XEy{AzxfE(B#q;VgElC8I$j@84^YNme7N`Yz1ns>^c}iw}hT4eSy_iL38IJt9&|@C`$Kz?t9V(Xawrxs)o3l6>NY>"
    "kE7KbbJ9m~ceeEae`Mftz&-j%u`iBAjpQ|nee8l*%G^2G-c0eC_8rpMD}0#A-)a)aw~kjR;g0~#)1DgZW*y~jp|Lr%V@c13Pt*IP"
    "i&bQc${9r$-93UXlL6X{H44^#T6fHS9q+=^MdW$z-?fxcJ)??i@C(50j{pnc#bS{#h$hVhN5}<W;*U*U1;&bbhY8HJ!l8o$?FS_r"
    "kQXU*px)v4x%1_wShV^ymMkfYX53*8q}Pf+va#W<vD~%P!5(~e1<%SxjFhlGg=Nin2X&wChsp6404jb!6EZ}iYohy|6TU)QVEi$!"
    "hA<AUUyX`G4SQ+KxCNkO01dRhBh^X5>_xuMeVst*CXEm7`r23GKJfsjc{pag`UY5IYJZLcz7K9c;U<zbE)~F@wx{ws_H403<txEo"
    "F99?~%tz!Y?B8G|d^c?J!D5I}@S<hf{66<z?vAD6lN8T`zo*HC67r(z`PJ9s5x<$;pHW<cuO2ffW?}g+)!`4tmfLBR$ZV9=noDW3"
    "7e?q{=vBDvC?B*VqBjNZdtPbRa8~<1_uCSXu-5U%Q45Nng&G}Q+DVFSH&5=9r)^4MP#_mQX0_A$rR9N#j8uHlf;cDUnfnElA=U6$"
    "EbS^f4yZb1!w^LfSa_+AUr253#hbj4l)4IwhSuOP-TFX6qpV`L`hD)7C4Hs1t2WOEyXW9wiPQ`5B#>DCx^0OTJ)jbU@0~LxPQ^p4"
    "W4wK2H16Tis%!AONNM_nmGbXDr(+ZM!5+*=$xnpB28;n7ed?ggEdH}n2o%KfP-F(-vQuLli^Sxd?{i;8%D_K_A}<YJ{;<aMIf}DQ"
    "S$!q);sHIYTM_qc^+)FyDIP0?Wx<V(O-Kmhn7dvfoz$p~1%wsLh>OP>EOlOUcM5KT$bsdrjosOH6Rg}x=>%h=AC~v(*rb3etq#ZB"
    "`3*^dLT$zMwcAy6GA6!QJ%e8YQHfpDQw#zw6JJLcVk=knNY{_t8WED`N-y5R%WAoVEUt;)QfeMTQ;QL?2$2N{537qBaF%Vc`6Pp_"
    "Q&4FSf{n@hmbjH-UumX;IE4SrNmt5_xo=x?0l%-FQ)~==>kA62=9?#)q*o*g+_=T(T8+{z0I+$Ui5#sRa2zQ0u;)9ZoWNhL3+uJP"
    "%uM=dbV!`Ud1alnhhy%0|5}hX#9UuXubN<G1tA?@kQ=exDTB;5?lwRlRgg9K2@`8*=aLYI$x4N~s>$Zu4LEWjj*Rks?(0lZbIye;"
    "rRpA=DxOqmOMR@mTBXbf0^M1=fK?eKR<zRJ9hAHu9Z2Lfn2P_Sas9u|{qAX+r#=YB@zQQl8+}A6QP*69ZwMBEqet<`G5D5MMDH~X"
    "2H(m@N^gbidhFL42$@rO9_pI;9xzfsvPREIbj<HL9e#19R_nk27{gmiqYSE(f*&b8AK5(G=(0qEPCK=v#fGL3NExKfK|S_W%l@9T"
    "@;&CmhXQw_59LO%iY3x4T9JrY^#Fo`u$BW=l@>5o#tLop<}{WL)iK)HXRx|Eh-2<|mC=8rin*3(xyF(vq;1(X_#UJ*5=GzUC^z8+"
    "VFRjX>@5ToL;9%2_0;v}-x)xcJQV`*c6C$CFS0M!HTV_ah#)Lhd=vj?Q({}NQ{VNf@9|3wwDw`2S35Pa%D2m+z+#tjfSNRoWkx;o"
    "un8qV5v3!d17#6+E~KNA0JD-~?t2;>61C?&@u0jUF9Td^GEdn$2DjVUVN_QC_n%%$2fo@APMKj7&3BN3o>~3&I3@IkS1@R;$v3i>"
    "q?0BQHbR2L?{lZCB2ORGN9r8Wa1~usMq_U<J9Ea<^J6`*`+3pZG_$6w=0Oo<fWYx+9aHCSeV_Xm51O!n&-KRNc)+8t##3R};I}(2"
    "(~*nixhDg#u)U#k-&I2QP$u9^pr1TG3mdnkOls*3O3|N<H-4~C@~FE7$K1E4gbGcBqU>h@X`Znnw($s-;?<Oq8ZG5ztcL~3M)05z"
    "38?&jyq+l9oq@TM<7?Mn83~V#T*#`_+dPUW=;|}W(z^BMYFJ;suXS%v<8yc73T5SOaL6<PD~}3jDRv%tjkykZT3u(+O$nc#tG)ur"
    "w@HuDLFL3b@gcgF4i>3Y^J<Q{+gqi@Giu()NiMSr#Sa57{n`c>11KyMag9qr5h?{!+H4D1c}z<4gh_BXxH!Zy_bvO(m3rWw%p>96"
    "@biUpjRtTSN=9PgJ+?D@j6s#YFxJ5)+9K=+YctUvm2EsgHeftOK*E8g%`vyU7i$3wvG|_*7S++;xwwd}yd~_dP)5qcHCAq28f91;"
    "6&o#WUbo;?3G>O~Eq^%H=_X)@J^MYK$wFGZAxucK=vwGdle*&=Lf-po{Aiv$xB$vEj#PZD`rP{JmKxi7P~fU#Q5s>D#)i8}+g+nc"
    "5jIYx8QNqaNvjB<yV0<;IF9e-JwksB(Tk_Rm&oGX)-3z0;v1ZA0gJJ!+_g;ZIU5a`ggrcq3f_zU)6#Tdy%&5Gvd4jdJ6A*Q+^No0"
    "Hug$lKb)rhG_e35#rpjeTdvI+(}+da-q5>5f$b5%UF-X;ez#Pjii_^Zd!NTsDpp}phj}2OPP+G=+9+<<XJ`9p5%A?2)~pD+l&hiw"
    "84wL!9i+3GI<dvnG557NrT=+yedv0PogV2|Pf0Ty1a8F-58Qlj`-Q<k!L{<@#5MR;M8V4hnGbA5qz8K6^f!Dee6-RlIX*>TJDiyn"
    "_#@y`j6=yrY8vJd=~PQivG3SML;f9|lO%gWk-mozc=3VyS5$ti9SO4GkHTYRqaKcJ8K9u}Pvpf;Q~7i{E#K_0;Q+sds&R{&RNsM-"
    "-uSInp7I;dgR=+dsrjL7mZl)C(fJpQs7*qUKw0WWceKckVq}U<k1ZaOgWrU$_(&9K=5;ZJ0}*0I&5Xb26y4tstn`hdO?eaRf^>Qd"
    "@5QjYTme|Gs4}mdpAuOXRVziZsaOMeM2rUDjbvHaY_T@@NlrAN20TLSlw-r<EOmj8fYLkWelKoS5(W4unb-QNZ7K^S`jHv!tT@gd"
    "$rhks%7|ByW}HZfM+Jd>(#*vuhhu9v=C-R60nVb$^}_2db!EpyaUGRuyY^BIA)KrnZxOgM&{+l_A#dG!l60Uu9`cWvl@L_fNN0J;"
    "S%9rOmIG=6`cWL4>pe|S-VXV0PxUoNH^gshCcDO7^V6EBmE&2W^@YI7FO^-KqlT+)r&)@+j#KM<w3r((^ke&5fgn?5uL;%ZY5NdI"
    "Tcq8OkJPRLm?=Qy3tty$lZt(e@(W0u-F4sRehv313`wC!*Bg89O?fW@@T;z|as_@Y*Wx;$tFMSurcbd2*X_==sia(gie$trt-~(2"
    "y{P6}$E-6B`txa_QsEfLFlWSw_`q5=L}^+p9xH%*OC2}O*RHaa$1}n8qwkSswt|(OTx;wt!bY|OW$-ka@r>eKpOh_sD(-XWNWTX!"
    "Va>9qE=1*nje~VaxpU1O`7EhTEXHJbwG*KvvqOhrO~23m9wXH76o+}_lrZ?->$j&<*z?BP8xBe#7$zcHA?zO0pE5Dz>6(B$YQ^SE"
    "0a?0Fqu_wB8X-}Bn+lM4)pR+HYvkjY`yL|WTOqo5c4d1Tlv%bCA4VK1J)rXB^>_dits7`-$yoRaivqE!gX2@!`w?ma#NyRu>a@00"
    "9pB+#dp9O;Ywdv-$md9%^n;F+G+ZZ*B(vikuDL8sIvxj_n#a6n-J93jC$%t>Jzn3Ax%Gt@g<k5s2c(6S(93Hl?L4sPWo8<*ZE(?c"
    "(n2oZ=NuMLXc#~Rc!)zAM^TJ$%>5oiJXc~;JsZ2pujrG-sxI*WjFx+ztE654(NpOpL@8dOS^HXvb;F@jB=T@s0yZ0dAkEY0ywoL8"
    "%@t4-k$5@Qsb15=kW*r@61F_OdlKM9>v2;eU+SKbU9_MWh!;c{V9}-1A`F3Kuyv4%pDmm&j=68g7B!jJSH4b&2-V>cDLu1AS__vc"
    ";5&sfi8ay(@k~k@9W3;O7tU3H1bnP@$1;CKdxdf&kzc!EiBpI~kAf5!mJbpKR&@uGZ-?HqAZ9vfdjeY4Si_1X7%8g*7?!@Dp|>B;"
    "v@9fxj>TfPUpN%(c2|payq1W(Q$*x&aXrgd7f&b(N_o;@3yGsqgaLt*?&5<+yjY&24_ZIjXw7<I%|G}m={(X-SKSE!sW|4ovU=5="
    "MOK!768fcyR@WPQ`^D&&9`Lz6nDXc3XE{AO#Csbhy};Uo9qc8ea}}~q6<Q5#D#y~mw6iH6|3(3eI)&=j%?z>kJ8QA4Z3pH+DQwq|"
    "e$SyZyJm%Moj_2aQFG!eSA1_Ewx*-v^QO~k22+U)&M|1C?_}Z%p%MD5JYJsT^X5q5y_I8GS}{+}Ck0UN`_5cz%`^BdoupK7HIL8)"
    "XwEw^2sz${K1C#y7^kH+awq=CF&o0D;JN1GSwmr4eq7+wK*KaUbY2cKv(e&v<xL9j@~*{r1|y#5eh*dYl~wied#`;VkkSIX7n?}m"
    "^bM7vkGB>L!YOjWlmKvn-PlI~0?&B<k>zxC%-ycp4Q<AQ>%rfI%g$FUlc$PYGtU_K)c6VbX}k}_;-~#m2Cy^s!v@d*<mmqA>U4u}"
    "(ZeXu9>351mJk*oLwnM-AWPIubr9aWo}&2f*;s+mV_h_oMV0_&JCSx1Ev!&@2(b>0XY-hjxnFcvXkhtxW=?8*81kQP3Nfq!Jb@q_"
    "_8+_LxHe_C$1IODc57$#-g1v|7wb(NbH74}JTFV4uHE$>gn*-KE*|!RvF}}bNeREOxwa;gEf4yNQ4_U!=yAGr;5^>Mb5q9<*$dPU"
    "%Lm&;9xI}d5z1NfPFfExA$7He7vC$*9QpugIit`<FgW&g4EF{#8gZrPSH~A6@c5?1=o<Sw+{8e0X{x55lO7V!^Y&eX@7-(8rKm^E"
    "gp}c4p0fMYb9tI>6*u@XZ-|0t>|2LTBXE7h(h@~Hjg)d8>M86>p(#*)?)$SS&i{zATCcQRK4$daD^J}<>}h|W`>(DN-qR%WJoxSS"
    "Ds((o^9{bI@Az7HW1jFG{|;kFfR9^{PUahy!Q+)FwdhZ!B)H%ot;GA+2}-;A0T%`pZ+WD7Vx_-x3C%Hg`YFj)7A1Qg{Jp5j4@qp*"
    "waBi(5s!8ycuKTVy0{293(G0H#{OEXt59g(Gx&wx)TyND-@pG%it@2DYSUw-Pf)kHdGv_br!>{7>6Eksdp>DyN~fczYzfcJtE}&H"
    "|JZ3o7ALNn66tc?=+PK@l9^bBlL4IA`JXW{I4}<<6&wWcw659@xX5aFOq<m)w_O83dQZ|wEo}U;JY6vEx5o-v3?w!2b`~pZf%vn^"
    "t0OB6dXG-_^XR14Fu2<Hx!;UO8(iuSctUV`^}h=DQF!bXDq>H<Um|+~qfcYiTCl#UUA<H4ctOcn^C0v{0X$0J_K|KYVf+t^j{iFM"
    "uMeNe!&5Va;3e0#Eb%}F;HG5o<1x-0(yQ_ge(KUT0j6eSAK*OTMUm$3@|-snu*}S(y@ds5j=6tQad=nRBix?y)t6?gn3FsFlsds#"
    "*sS2$ocUU#6s6ILr#u&f_16mn3TmE@mEUu4q%pDrPfH?=25uo><|&qM<{xp$`bg|=FuyJ5WSRjsRs<r<R2%HT2rL^Ds($OHq72oa"
    "SIcQsaI#WM{~u1P|8?$Ht|1}bJr}44yCxoLvK-bRVdV%inZ`5ezyZ3CG*&6D7YaLyqz?KqRay@cB{=4mn*mZVNv?Hr#gU5rxwZ8S"
    ")|>R`dlNiDR+Ps7oq>o^)nnif)WUOOeumIbRYcN|N=&mp7NChm?6wkZKnq?R7*(OGDlb0jcXX8B3sg;!&Ub|Gn`es+!Hw;QBNJ?Z"
    "DO%6;HQsUNT`C|4+XcXRo6?mK?|QSPb?)y0rM#^QNhosW($Gz}%2p|Z(EfBAa2c^19goS%J<CU0rwI`j6IVNcC0C#xu)#4+@m8@B"
    "z!3#Y^!qX^?<G|O2+Qi)*X7-s2%_v%rh%>HvO^g=$I3_Dp&7vNh&y*V>};M151ls+FDlY>?x4@wS7*`H`A*A=+#t-uTlGj=r*eS?"
    "2vekirE9Rf6<(3GfUYmC7n-q6#C3^^!M{5T5Kr;_<QIc0MJ|jwZ8Os9iW2;}&>xjfshef|y^fXQ*=-`r^gI@jK?YIL)i%iP34`(t"
    "*)jK9FUkx5=zH$hf+FKXW06-+9F%)PSR-#aT|`gbz9&3>;ZXvMo$_W){bYrU%o#F;*r<^>0lBF7QtJfjJgR-`BQ-E%onSfuJL9{X"
    "1Lm&Rra4e!`rm)r$)nKfy2={1c0@2JbU6e&k``G2k-?aNLnnQH7{nArX_P_n`u+XTN#bPy+~V~70KRUL8h!crJbkF-h1Y4koC18M"
    "$f_M`2^I`0bh`y}V}l(QP1@ht+OO8NJ@?KYmMPb$iku0|(p-aIK*+j?;IbA-x6+jilLu$6v2q1_5Jl@_)p^d+HByiG82AXQ(ms(+"
    "#|ZWDurPC-!CIiy;&Y1JcA<w_+%tJr*%f+bv%q7B69zy}4ta*y1<;FH)NAU&vO`9s!SgCl+{g?66%m1&X!E48+9PuPeePGzuwivu"
    "^R&XVq~@*Wqt?A84|*57#p?!^<T^LfAM!`|ttq+&rz`8kYG@2a?wi=m%G#v^-o?XCCd9EL&yEWV4>v_}u!DC8WZH?XjA^#RBgpEQ"
    "+OE(L&;w&A@-W!*me2_;GH}B{gb~J=3g~S3tcdei4XKw$E4s5*V&pyy0I7T@^;hJRIuV%LQ`5ntxvPYtEK@Vr;OjM#@U7IP)6YAt"
    "_b@_LOUcnS_;&G7$1r-zO&Rk4EN}Lfhd;OUj=^vLzEU#T#mKKLD&1IvtNc9O(#M!tM-*J_Qrk7iq9TOzsmbM^BY!Zo$j#(=?iy|_"
    "b~0+DgRo!^(#svJ>)<so*@JiPU|@LEE7h6i1e*r2p?h;fyaaYCIiH9qslze1UQ_$bbD!#Z@b`*^vQE`gf|7nkgzPjZBmth$`D)|I"
    "b9+i-gI><)0%pw8mDA3cyf2>VsR5?uLLyHtWbBER9U+Wa0X*p^KC@$PyY`3?-@vo4t|055^AC29vRkGt!9GD6rTPFk$=H;PSbGC-"
    "j=k%NLn?9}XuJg~m(_66%q8^U_|$KzRZ<lryqLQfuPd?C616s1iAGW8VTbxaJ<0(bU-U#td4k-s?;pv4awS~l-{;o5#3RDLdCDm;"
    "_}7E~I2gOu=uPtFsVNMuv2PJLGExoP)Bx3IZ;Q0<KGb-XxE?8^BcS$E2hGd+LUy&ZkUaay3;(A)8`R|>#q~YcY`mq4#kIGtj8zVx"
    "@-e!$*4tL1Y}hL5df9eQ5yVns?fE`bsRViltqTP!>|{m2+w?BRO2gl$7W6F9IxVwH%2NAS@#H)`;^(QoQIzGkJny5vkQLA+WL?(&"
    "uO2Ocxop}H*udvti_7D~2vqy@Skgth@p?5ZV2<sW`{(4-0FRm{{I_3M3G!ug^<bu-3~h|UI!@20Zt3pI%EQG#S)|fiu{ov!*@MS{"
    "1?e0%Sa~;<Mvli$JosD<!4u(?Qol$~OIxp^j=5i&^|%~RijZYHZRp8>^?h%|x^-BVSJ;D$r4Z(hV7B=C<ep}9j82<spl6yx>^Qae"
    "zPTwlfVqHwdm}h&W?4w$`qbA-g2EbHEUv+~Uw~%`zGM4e)uqz%LlJ-T>9dOpZ_nnS@Kr;0>Dn{JC{qQ;%Ty%n_{TYgBa^-iY2)vl"
    "J&xm3>oxO1K9fgdbXPJvd^yp>qaf_M#?!j7K<CR^%8HV`$_|vdsf~T~GlOM<n0U^8T5EsVm9TQh^U~fp%BnCbvUswVy$3%^cgm-O"
    "sMz3veWN!>XrqJLgwqC6`Q%Y0FLK5x2Oz8)^GLwr?;t$VJ4QJ+m;Jm;3$4$J2h3W$@)|6%W&&V;)uh69z#E)}UJjx224ZKV!BVMy"
    "yJ(~<b)|zBcK|5T<aNlV$>sOC|B6VNYV2T%0ipN+9Bo!?<rO=f5p!<?Xo@nL`#CreEGg<9cY3P>D<z6Fu|Ur;_k|e%bzpm*{dGln"
    "MX18!OSyKNg(ZUj5}5%e6*{Iq65q5Qz)rBNKp1UA>8zJqfh9sfviKJf(Uyfx>=E!e!uQl=*Ja$O2<vKLN<Rf(JWGzg@)@gFn?qUL"
    "C9d7|sy&cZ6WP1Q-unh50AD$VT{la_&D<Uby8-O5S=7fe_KFR+N;zSK9YQ-;h=(dnJTqBkC2V&pON0!PB7>Lqj=3+trS?bk{b>6c"
    "1G7NXx4N&DrT|;fy7c?td~0NlmygMF^NO)=rj}T#)!-U??<xup7R2|^@3m`W?GTUm(%2m#IUp`e%^G)F2S}(<yEpZejPi_h;N_{_"
    "q-q?`!3*+fMxxL2``lN<ko_?DgI&KmU8`0$hIr)sa(M6d*-;`cbi#|;`?jgO8(b4#XhT~yQ;+0hzB<Qb&@K6%TCc=Up{*mEGN@S_"
    "Cw>oAp*zTG9qb|FL0Ak}%pIAJ3OlRf9|DM`g}(#HGJ~G=osVPgm#`qRv&Dc-IQIBgudyZLPfkYl(Zc?!6*hKmFIonYA7ObyvLpHi"
    "w`^Q#w$k^x-;y*DfUl>NqC{N){DPk}a~)NW;SX)JwkWy*`+&<gNx)dNG(U;5hfR_+8wf{^-+kqUs7sw(g=P6ynG5F4V?Nj5w+*I|"
    "+GkI;!r0q7Bv1Hsg)s}i^=-{%Q<CG`RM_y*qwAb3#~(4}l|}O0K4S38EeWHki02u68<c5};<G5k`wiENEYniV_1Ni(ct2-N+4b00"
    "`H*L?ymE96zMi~FuiNGttGDow3fbm*UstMz<?1DS#C>wS&48(~5X30%V^pb7VMAUP1=?nn8;H6-r(f7QpeY-nX)N$(M6#pSCr<zd"
    ";^g;O5U<oh79Ify*Mnb;dI}<$M%TA`FAFqg)<G}=mUt%1b--vucBpH{4Z>L`(6*e@Tir(xf`?_$SZw4&QP3J_>X~x>KKBb=snr`@"
    "TJIS9?km7gYc6-Id`o&0%RmGV?;8Bdab%@kHQqKA`Nr&-`^1j5Rfh7;Fj10220MuX3?|!OW@G=!ZU?KA4RuNSijKM8YYIALKR!AD"
    "(`B)|SBVQlRHRA}>Vxb(XeYZXYwTI21Y(FxJ^DoA+%j}jcISVe`@L}mwlDZIJ*(_0N>iYi?jt-w7G829;}>x139{Ko3uVR0<lfmV"
    ")W+C{mcEa1Y&Eu}hO$#b{x`?m?+J1=kWyE|N-qy@%_tJ*8+$Dksq9`|@9Vu=Decigac$|J3^w8~(z`3mrnitRu}uE4Chw}Ji30Km"
    "!a*#lDeIUnE$5N3-t;oWUrY*Fr-6TmRb(S~prrHt_&EQhtPAg7c%+!tq9qOX>7gn2PH9y9dvtXYNjj@_?J=DIsyQ=lRl)xepeXyv"
    "9<=Xszn3dXjTL;^6%yWH#BE{hPw))>-BOgAX!-3LoUcxqApm%~WHI<fMU~A4)l+(%^BY64tFSGTryhVkqQ9!xz-Cw1;I|qK8CV{P"
    "v`GVrtz+(tJe8>tnRtP5a-%PCID^UwU=oLYy@&l#{UG+tJ|ie<bIkqIBzoFdT(9ibtnfyg25ikW`Ylh&xnR9|ZszkN!Tl666hws@"
    "U8fH@T{UUG@{(PTeGBD7!^Fk+#@@a(I(%STx*)<&#~Qq&v8c;6_*z7PVl{Ow&>P1HZ19_1FZ)%Ssm3ko`^CRT(M*>o*LQj=Lu;j*"
    "if?JYDmTPAW{<~p?QM$zBwNd>9XwenGfjxxR6wN9qL$<al%z)DLekKzD?~bue8-Uay&qQkEem<@ZmU~GlIl}i<>tqt9dr@NtP(v>"
    "kFM4Am@<?&LZ-9cY^*AoLB8j!e1k7C*HiiId1v>I82if}RkyAy<J|zXfmP(*k!d^29O4Wcf$C6X@rayVkdaqTJZ5QQtrZp<LB2zx"
    "60XcIXMZ}Z`k5oGObx^^fpp?7$n4;Lg1-~9?Vd^s0Jq4vGgp4aSl4Ew;7Q*P+5t!8g*E_^iBJ&C0>8$9vFhYOPZ(734PmXEH-1}R"
    "GARx!we1buYHFhg_6u#^h4$}r{|@IKMSt}mqjJl+kS(jrxB+9+)iw_Dw6v9aI{ug-3!oRN3K*h<Ls1Gka3s3U&r!!|X^m3$n5=+0"
    "eE--;)6_9{zS?XQJTTXD+pSjt-AFDFTi%O}TIEkb@(jK**1R8<<f%Vqm-{_hGoCs%*Wg#62FXei#c`+`FtFa{b8&&cwT~jI)R}ji"
    "v&>xZtr7qT@*@Hq_hcI-E=;|9{cco*xo=^runpU@#(px?f#lOi(BxMUD<G4>H})-QO~(36R>(`>Y4)K$DE@n5F&b~z0erNraj^3c"
    "*J>KTY!vCkj@SX=r_`Z3_IKF+SEacc9^GT*h{3P-JkROGg%9Md3<Xchti<;XzCycHQgSU%srLZ((Y$ie1-ZM5CcZUh*LQBW3xlhR"
    ">esznI65+eqt#OzZ~U6<z7VJj#n16UGuYVJSPvU{_UtpeZhWgezUz=?xxFXSs43^svdemHb6TS_xyPrJeOT8gnzt&R@Og!DHFIkA"
    "CmH^TIme8h?XF^{qy;QO(G;4zP^o72K{3i&jpp+w2UenN{DbZ2-^hAhrg^1I!ynIszqSjRCFr2quEB4@5jJX89>P9_ojp*drL+4F"
    ">9+!fO&031^RQ=G$`unz2XR|-tlkHZ<BpFF==Ztnf3~F{0_GB<>%s5t2>4+1FLn)nUm1{?rod9%f)t^JuJJknTuv|<z%w?0Z6+_q"
    "fmyfBv?7mcCg><{(T1GfRAY3^{nk^3%KHgdvb@#HIniVzx>&;%Z>1rhWtYV%FYjO45Q`PcorFeyS@&24Z_&!ci_-91BAj0EQ9&cD"
    "fJp2$;@C^`&J8)4$G8h)zR#U+ufDOXi%Dy&U1V)ZYl2Gbc(7V6b!9J5-X+wsW#Tsmz|<`Cz(}3O+cBmFu>O7SpVeHJ*jy))_4Y!r"
    "8zG4L2H&E+0`$up#{#erZJZhBo{NEY(W7MzshRLwrU9X5M6WOzoe)e4?|oZt#FF3RQN7olJOl_XCtVDFy~|k2TbZ_mTZ9V%5JItM"
    "+9_*RUcUhGA!<=95Q`<x1!Tddym{~oLc?avZQ6IzP~S2cfOU{mSLUhjg=ga-X3nmUWw&FJU5Kn`#q{_{^8hm}qa0+%ml=-{-b|Cq"
    "4+C)}v1q;Gt(Y`gXe2PLQ42Eu=egStOI9Acctlh&_|-*AIWdrW#>U?bF>f<m#~<ysOVUI@cnS+T)n;4;oN-~J<=&`dI!+frmz7R3"
    "%6tZ^0IQF>>ztKh-e9~yrdGbs{ob}3C@ua(asBEy-3rSd9y8ibu~`5GZFN>Qp3lvW*G-E@s?WwuE4xey*t#>_&+*@|K8|#j+kZz^"
    "?QPAfSl=lnPr__(Zxv8KNU@n~@GZsV-Y{1WH3r`z;LP4UR~T$>)dvwBLWOH_zhO+XP=*3&elN3u9oLcH2bRu$YTq}_*T$g-v@IPJ"
    "&I6D-;8tzLkr&90hNV%}G51ffL|Fpj$<*x)W2v%q!(6<fy>aaE=P-F(T`QtQr-no9YnBQ$H?Z`PE>5E5nw%0R-cjdG&I0b<X(FhB"
    "MR(peIOe_z8UQHxOV2v7c>CQ;CcXBIeI?rQdrNYepyyk_`*d=I?3=gU!VPK@xC%GySGdtiqXngFaQQh|A=78qd5_&AbBYHAPa7PT"
    "<2_unOmyHHdo5KlQktut>DK)w!4@?(jMrTj>s7Tc8t0w-IK3c&2IKwy%+!i~9I&+@w`6M#Kyk?2JZ}2lkDG7VaC)5AiVFzZ4Me<>"
    "K@4<_eIX6fq`J%pF!m+Wl-8@%vzhEcFJ0NO9`^1e`vw2EdMq0`sUm#L05U8xENz05fQ*IE+k24jUGjL-w@VXABR}ra-v_@(2!;(Q"
    "<}t@8xBiyGYW7Dr@B83*TWTrA=GxB~T&`d6V?V&xUE&#28WImUBQT#9$&ZVcq}lS>8aKT!!J|{O4(MA3HdShqlK9Ip_Z5}FLdksF"
    "0NE{;PV4zeXB5_HFah++Dwp%eo!Lweh#{2&JCYQT3>HH?AWn|CZxF1NxwE-m+V9S>{1K7!48Coal0enObA-SezxQssQh@{OB$v5Z"
    "6&=|d0tN{pvKFsz0M~o&p>Ia5w5M!4fOIPlHkN8PqN<?Z=f3LZnct_W>y_Oj^AMCLqn=lGOEYO5pP!Pb1~?i+M>IdI>qRCC6HsUy"
    "*2M^I*0fnXp7O*`9Yb}|k#->_%pO479CKfNTdWlIoTc*E;U(g%Y4ViwmtT1$`zSS6*9%KGRbqt}k4#CqrJ3z%$F=Aie2YQ_WH@?m"
    "z{;)7tN^|(d-Q)0D(c((rhzh&uEDSNL}F)WV-FcGCda9o6<K%o0j?jDOyt2fWNk+5v<K}D0aG{6jMhQgn<uA0A9=^zuLc_{N*^=7"
    "a1d~7u}A#lNDF1#lo>$F%3xz&470Zob{V>iro#bjJg1JNGj}k@+*g(t;p9<@I$>@KWvzZFGL1iu`L#2)w!VPJ`#_2+XOD@nyI87O"
    "%9_1^LITIg_qne=Rw3qhbzflcPjHnF=(!y!@tzb>>I0j@wWV(fge2Zq2G`j8laXw(&s>AAX)ji|`6{v4uW>bIYpUjoC2wmdo*3c8"
    "_nqvXm<a8+@TRl!h8h8pSNlmPQ0;82#G#&I%Ak7njFahn(v^}4UC)s)=tvDb^0~dI+<H=_;cMgj8es#@oY7AdfEy}ATBQFSDB}dC"
    "v7@bP$rH#k(G3(3bG1hoMAOKgxqq!iRg!whaJj{-kN>!^D4)7*;J6$6-T;Cq2e>y<DxOJsER9WnLT5|O8N2t4_lV%3<SR-8<?nM}"
    "D-ibk7;mzK-%jZ&!h(5We+thIfxJsS%i?nqm}w+b(oj~#r(P=SCdMO@@0j}y|3WVUPgPS%SC{Kg8(DLWy*fHXN}lsXjD0s&HRJ@("
    "oj8pd-qQHSQ&4Ikvt2zMMAt%{uTAW|t0hCIttZt@moi#3g3htRX>As*)=f=ve4O-sZn=g`88N}-AxjCd*K`a>PMvT2ma0U0#gQ9g"
    "ElJ*$XIo0InTUU~YwUClZEb>L#DQPjK>Q-_pLRVJ3k+{4PLT8CnEO^PgAQX&yB@sW^Kz?nVnw3Hek^#Vy>_Kxz}9RV&$m(;jka{9"
    "@VKf6pwNt6(q!j2av+;uXMcb2{PtK1%ch?K$*o)P<ICu=&DE877r<Tu{yO#=11%!3CLYJuG9vpOwImC{3~XwApZnJ3E1Q!%LTF5{"
    "*Xp1dJ#amPe+U@yo8A(x!T0o+o!YKGNh%7y$B|?l>Kl7&z#ii*&k<L-lF^_@MwzI8|Eb6Dnd=mwPTIM%yg0EEp4KEt3!pQn@9-!A"
    "^!l84(A~V#j^mgkBwXERUQe*P#uF0q?TlhRjh0=jM6O`zlWAP>B)^di#F9P<$@RW}jrBpLWf!=~?=gR2Bzr&=;?X0Wgblme9PJ3j"
    "oUm;&$W$hC1&Z!v;;&1OylvhC=_it10N;1V+^^gc{#11xNtP>~3{uWUb2h{ZqSlBkq@k&d^ciLn+7D)Cmy-qXgXNS6B&T6}oH$iW"
    "13{Rnd~Yx+w<@(rOZD+nq8(vAF%IA{EMI`Hv-+soBcNa&)N>kiLynxa4{~}cFi?teihWl*Ww-PzjmTS)$9(a~L2T%!9e|EG!V2kZ"
    "T7DzeiAHJ{1qyAdBBtlFTC!O?9y!v7^m_m=H<nSpA^Tit%dKezWdJqg4aG@Y@$s<AXfA~WwCC8ZEu@v%vvijBD0tyy?G$fLkU}f?"
    "EA{Q#{4TYPx$9p=$TSo2JU9>hSuWEu&ElU*;O~Q9R&MkS3Bfm3FD|27o4Cf-A9$xIYjF*JPk|$lRTbCUx}GokxCgk%<&})4NoDad"
    ";!<y+VxfD0>j&HQVkJ3Fu7j5=`3&F$=p8(5BRo01#l!4Mb>$p+Q4KY*@{^@|8D&|kpjju5P|hOGUbENDCeC&q5kKplrMJ4nI1G~&"
    "(EQPL-Hy4hZy{b9kf`gyZ|h!qOk~N>##5p-I&VpL06IRvr0jJpYXg>2OCbOpQgOj}SdL^9`x*!a2e|e-L%RybtSjX`h3C0%Q4)Yk"
    "b-CFUd3Uq2*rd9$oycu<jQr59dzoJ`Y<c{4lWXRCold!oU>;2@8WmmvoY(^Eju+88^};EIY}<CXxUgw-(AjHC2Z6@wo%EuZmcbWk"
    "mxr`B%R08c9|GJeUUuyP3%ZV)uIRERkT+qc`D~r%rC6chax@Z4U_mjDNkz~CR<{j*(bop}0QNlC%>cDw_1$T-Qyg=@RpwyRA-ULw"
    "zCA1Yczv)iD1eK00l)#`u$jY|Er_*M!K<3Gk`MV7IA9mbT1Vq$nhG1q4mM#o$K3C8<J|*62G?`fUr{M#ZHsI0bzGqYczHTvM_p`u"
    "6~qZz$DETEfW%=0++_vINrKj<5hNXaE;@S!8GyZD5a#&NSC5frgRBhlgcE%)8t}*9%~MV*^Pu`Dpz%=FT4hz8pu1QHX(dZwNPwF7"
    "f2`iZeqf1gqRF<jG4T@|bEj*}Ni);rdhUGvEvcsFiv3q$J5tsbB*3-6ue(m=t(Lf==p&K+N2vOdS#cQ*(;n}xQ7U(dYwEX7u^RGS"
    "R@dfu&!3eRW%22Y%7emy2#hkz0UZ8wY9nC!b*(&OIujoi`Euu^TjV4*p64S0;6NR##BK?h#U{~Hkjg8OH+NL*>*T5IWcBM?ej)S$"
    "b4~o7*npK955fQahpM|2iET*^-9SbS2Hdd^QgNcfXqzBL<uvw??c6A@Jh2f&v3YVHj=8TEFW6UTuIEnI@V4n6#n0B#Nx5vKg}64h"
    "e6IuH91LoQ3C^4!dnam&>>j72po<}*_AY?3_Wk6fzOjf52RVQ(hjN(pLs)rFu>7{n%8UbVXCceAMT3P=fFe5$i)>Mkb?j<3DLnW+"
    "<<)!d$tp7NBu_B<$z(@l%$z)O>eOy|@0h7Q=PAbmJ-N_5JVGY?T4T1VVB)hQ!Dt&|Sa`f!NPEr%aA3XEhQu?d=~_m>S~R>vBVON*"
    "x$l{ek(2ti)>~9c_p0b)O6|4@MPh#61xI^(<SFfkrHeO)xBdw3?HuuTI#zu@Hfmjjg|zPM^x)|<q)s6IVyDZBbfqt$%J)_u%q{<E"
    "dSOX`^WL)ELK~IG{ll}X+nZ^v#fn_*NtW$eRw)I!7|B?qkJo{qHTPL3OJ8R$&?9VA#<OmPH(VKLc@Il1jvi>b+&Zni!}p?*TezI?"
    "2?9k@JP-ame?U?~0C&CadxikiqX4C@!7rC@4J*W4Y^VKFZHwAi5;d*uve|OkYXlsdnPm(3n*m(^tc0%%-fnyBIBUpQy**XrjYeJU"
    ")xY!7pAK&T6b#LERP@uM4cw!_wYu#sw2274ku{v2!PnTBg`L&aj6nAN@)aTTplE(9WlSAMS?>hCnroh{BaMsNS-ha_8>DQn90n*^"
    "+AZd}u`3VWepScZZ%tIT;wy+Pw~7~gvXRTW-k4lzmX@@sD`DAN?LtLE%Aq5K)|Lu`A}DAXoo|lb&I=n~`)t(H*PiAv5bn|XPKUDS"
    "NKE*9Sjpedf9>`x0%ib;R<?uT?VZ6t6kyZrGz9S}cq=;&a^ON;;>xmwt_BtcAeiPLJGJAf^0BwlhMsEV=bm@=o?&34vW?A?vg}r@"
    "Bq|4~>+tt49bNXhvLg#GAMbn-XVxmpJ~!PhQ{6!|#JMXTakR;U{BqdZTDu4vTmU8jp&fJIaKhG9^ISa2uauW*r?H0uJ8x|^u2*(u"
    "D^KgO0-qk6$O|9>>amt+<S!b)iTH0*<p{mN#{NF{d#DMVgePZL0H9_pvt{7SA!X#wG+aPtCe_Z_Pm&sedQ5Ec0fH=HGGq@*bYQ7D"
    "=6-)S4-)W0lINwp$e+qVJ`FVqzxZv`-}m%jVDK%;N{nTmGIt4Ahf>N@tL_>5-gyH4w#tKW*Wg=(rjccQuE8%tkRtJq@klLe?uA5n"
    "ka&+EA0u4@F|HF=l0~&MZ97jq>cVbZk2*T2@g~fi-^+{Ss=HQ6^al!_=e`cqyq}MfU5mu-agp&Tn%&cl(uFef**zdzrdG#9PNXSp"
    "vRp?{8SJc{jjBVfZ0piyI=SPG=yJ^cwtz5kYV$B9i9xPrUC9rSq%{&Bodt16DbxsDFPr6|z3<g^@(AP5y<-9DMmki+i~x-623{rK"
    "=YF9(<-oE$K6VWmds9?E9zUL5WcRdrB7{&z$(dMzO!}A|1dWO^$tLUcPe``gam@Xm1vFxJ&#fbVsoqWutHHw>B)+0DEkrskcvk2w"
    "(NuPHL>}z{cjZ<x2*f}#nyauuEmLt)^C`f+CD1{1P^@4Bi_Ba`(gK*g*w$YibIT={p<r{l7TK#WNGCZfp0*c>*I%9T#rS;=ssk`|"
    "1@Dc6J@>STAlKTAtb$C3x0m2sbM9^{(2%=fkBm-bjUxa9lNDP5TN_&eiMEWx7Y%yXv8lhe<H{B8tN`6Ch;Tjk`=cNCYVlO#%2n>f"
    "DsQw(@obDMLitoW7T;69_sd$+V~I&sL__`nVME##({q3$CM73=mW7;NSIr0(fE)XOPo{n-K-SA-VZ}<{7k9eNBgpQWT;{$MmwxX!"
    "fMzpK1|t?5fF1tWNI9T(I_N|J9v=)BW#_EyejnpZ1AC%)Ck=BJ@SwFA=l8ka%P?*Y`j-OsC;oW@tOFbVPzGcmOs=##a>D`%7G=-I"
    "iijNe!ievC52TPrQ~CD&mKk$jUxvm*A5RyaT=mVATc*=*a}EBP0fBME>Kgps>Pw=mcyIxud|Tn71)k5jeslU6KVb!DkBkpv`fYpx"
    "$#R*Lv%lx9OsZO*4&+ejx{EhJM<B2Q$)~bqbkJ@GED=k#r`GGChzRh@py-XzU~JWnfBpO1R}Zp8@D5iz$+sj{u*8bXj}3$KwUnm^"
    "9xsa5Je8)AX&|IjX_4nf*ovhN0hgk<H89$$8>ke_888Gd0o483Ha$4zmP<`p3T&=*a&@Wz6sa8JiAMRo3QP2%%8X=NI>OGTa&@H>"
    "MrizkyHJ-#5M9~)fG<i(A8^J26oH(9th4b7*6GDD_X|XG(DWGcAbQh7a52EmcqRoZp0^oz_&}}rluxGb5u!Ouu@M6kfaUJ|P~XkT"
    "F!#NE6t>xWnpD9_Ul(X$$E8a+34_~h>ZRrny+DO`<$$j#?Y)Z3M(?ZGTc2@Hptk`UzUQ$-nR)~RTbA=ad^gehcNAORc+_}ThNSD+"
    "Amw@vvc{9t)-(8O>IMO*70)HLJT-si|2$3pS!oUL8Jw^GX`{n5D_3g8n`?4Ti)kR;c6uEEbd5zFEh9ys7a=cw8Zu?4<O2`|z5~0!"
    "lVfhZ9ahQ|;K@AY?d3>p3VTR3Lpc>Oc}$%a2p}8Ya-snw5IV015ST!kdFWIqiLVD~WbrtMrB>pY+pY)@PsKeU<qC(gd4z5L|Ne8b"
    "$>@2l!wL?-Fr7#%l+?(wGah{|Eb%-6&JKc#3O2M-=4IEBW*d&V)9u!0zo)A%K(2HsvIjM%UAV5?Ha<FK0d%-l_{(^hX2jLhCglnz"
    "g+Sj|1{4I&P01gcxhODkSo{l$uI{yv>DE%7GDP-dA5ir<5XiCY+X6rTe$&Cz;cFemRq6?Za!nHGXz^%WD_e`s5(OCEa|z3=3;xO6"
    "86>ioWqTsWVp%PoAUwtD2#eSVbx~N^G50O@p?sa|ZeMP_WCQwV5u9i2jf0J5_T=-q@6!~!I#LS7rfhtUZkQ!+i-`9a!z{cHHiPoD"
    "6}*X60NK})5gc>hgC9y-vgC2?+}B7;!`c<U#+sfUuufKNz2p57+RU+K<GTo@`oLA!?0MjsO}|G%dAqog!`Oo8*_Ky6l*VX{!ojY="
    "H<BUth${li%kw1W(jEynrD11T6UlS&e(SJ7Kq-O?$OM3YK>q1Zg6&3KE!}`spi!Wo;J9;t=a~AQ+XI4AkH@n>Z~dMES>;Gx%{vH|"
    "-)g7(Yf>zqnvg$$0tyAm4>hIgiZc>aEw-G_t+UyQgMHPGx$j+EOrd!0;zVy(0wFNgjBjwcEe(wr$%CP92_4-|sqN?*+x~+Ff!%qU"
    "=2@P<DmdQ5MVBiYFK;fTMy{to0o!hYmgx{bv1@t4+rU>=MiSU(7!9rw`UeYFkYWpOVc6yvkER0__kHdw^;Zy-)D`*l2fnfBSgGn5"
    "T*Ixw0P~5MH-U5IU8s=`B$QW6JF(CNU?OZFh&6VON|{ymET2t7;E}f;mZQ(P9aEQI`E>@vOs?mC?;8G8)p}moRSrcyL<r2TdDFdW"
    "sDQv^hhww6VU(HEu7V|V!0Zsy5eydKFV+qX+P5T@8c!Y-v3P7|Ry(%oxN4~Bit;8pc({%*>f6Id5aX;@ms^Jx0ZIym=RJe3=xF2>"
    "sk*N4ZXKHOmdC2=J72LWUR%`UQM{A-W~)>wcB;v>N^ZG~Qu~OjBpt+zG*mz_YClY0h;DLXt2_q;4Ld;Wn#0m_R^B~s3V^0Dv(hp3"
    "^W5!MTpAEalDL*h`^gi4Wcln%<w|H$$%qc)o-chhFe&g=gKy&ZE{mRj*czUqD!`KX?u^1#j>2PHz~HiCJuC~^YtLD+>1IWG0`Cpd"
    "0Wpb3=%^8q7{E~4s2!|qG_bAjbC+uoM%}yM$&{`9bl9piiLS9PVpq!ufz%cUt38b6pp3Q0a57-c0GT;!(pE%ivVl*!2EY%ylR~Fm"
    ">Y+t2fy(!}ufYu~;GWVwYQAS6zN6sE1%#VAR%;dzs?#KF1lm?9>Cq865B2WiVwzfQ;B9Ah%zZm+8@7h0Pf31~qq$NISzN386(@Lf"
    "xoUCk$+yJENNtn)=Dl^26eMFOT-fX+>I00Z0JGZJoyIAhx<DJRK2w*!x5|1wwF&rL>#1D}KDVXsD%sMmy?90HORhqj_Fg<97$7E9"
    "*T#6GZ^70^#&H1)3-^+N_g=mu54#3mQCXrHT!U}uP1G*-(f=zqN^7yk_~{yaua}WNKx{p$)ABWuBeycw^0cc8!;*8Ca;Ml;XF>R>"
    "E?+T>m3x^_pF?g7th0TrMcciUIixe{%PExNVvpO*$9T@ecDKQ*h=}+vW9s9!^qq^^#X~I7vFakxruQbQQgOpm#f!oEmqh`xq-$f;"
    "UpFt3AMs;dXR<a5>2U3*%+cCDQ+rBb&DuIU>_HvF>UGSR*Xg~m>Yr@^z1jD<Uxllu224C%a4bEzmz@ba)MKy6n)WwWQQ%8mar<h9"
    "lF4Fkc%p$`|9TOjw2dcK>DAt(p(N-|pa=vjFsz@^dhNy9Sm97t&T1Pxk{7CEjftdi#+jLWIZ#?=7oiN?_qkt@DH`qpB$?;AuVbGP"
    "1QNy#FqIK(V69ahVA&BcdWB9uuiVqByheN)V;?gt{LG$!0zti&r1f2|RDYFoW<{TCZMvj&&6(Zexgs^rp|c=B00xzexihHTJUcDW"
    "13^eYCJluAcuc|5I_AFe+IVS6itA(R?ei(%v*Ed4PJN%jGQN>QX9ZaR)u_Pb?2gPuHWJ&&rRKU9-S&ij%4f^zSZj`#R<E70wLi%c"
    "|0$Rjf3@>T2+S<J*4@sjEoB6(oy*<;9F(wWwakJLv8Q$8EzEE%w3prqJ9H}};lZC%DKtWXlYC|hr!3AZW#d<~h3XtTfQ@~3$rJx3"
    "_7=Np#+Xgx_qlKNb7M1i^r#bH@IBO1n)~|3z7;|tkq`&hO1X8CRABSfDr&q%Bb3KdSMeNs_I?_#;7XnzQH?LQ$wrjK^MkO%Sdrqe"
    "H7;B2fDs_i@LIdiGbt6%gW|Nv-s6sal@@pjtOY#$d*QR*D#_ZYT_0R;to(d1crNHEA;w_)HABc#k(Fjb99oFktodmj@o0x`0P$9^"
    "nez~Oh$CLI+)xrQJN}+&uG2PB_V-<V6;na9c*Cx)Pc7vq?3KZnvup4z6-N#WuB{R4uNMvPnl+L(ssGV<T8sV7RbVG0Y9kewO%0%d"
    "z>Oe9Ax6UjDf{T&aSWc|^djHqzGY}B(-Xu)#x>odD3;-Z--&DRyQei5wulzj;I|)`ph|)(jHP(5<1=*;s&DXZ*J<&&Ch!ct?UHmx"
    "OT6pE`mZxnHJrS2cVfN7mQspewN6O@pf}pW@0QpY7&chx*_HJs$QF*dujr{V?clk4K8MWdteT9%gM*@capQRm(6@|U=xE0(r((Me"
    "V{-xFLnjFK%>7a=QEE=}NOBsI=V3%xXKCmN6&0(VrV%m5(*mHJ4xqb**W5|vi(#1u8Z;s8b;sPlzQxR%O;03D+}2OYiNsXt6XhBF"
    "vX;sq-#iMQdSxBiMOQOHbf)iZot4t6Sax%sg1@H0pw!dXm}^-U(3mp^a!ZmkV%<6`qwLJtG5326@f^Hxd&o>_MexX+qb)>gJ1GEB"
    "h6AkI3>X*5DlCNg2+&kDw;6f>!&(t|{kuG_-kMtAuL?ZtNw?|hIq<@hBH6CN_tLRu<Xq25EC&B>;OMF83JUo>e2$T~h%RMw&A*7k"
    "9;4ubRrx(W5lTWP-wMsQ!ATp%xjapHpeApjIt^TlM+-u4St<{3sUH$tgRdP1kYE<qyS{BgNSiUaKxnzzolgrmi|etOHkBT-@a!@n"
    "_|Zjx7fmeu1`#}rSO@6;&=wP%kjDVBuu^x|GaPfv)eypV=5uevdIXMM^43&B-}s2NhLc7*)^t)DiuVF}6o6PPX(WMk=w_4$b!NZM"
    "{a$Pru=%F^-8@LrYPlhp0rw)VBGs*+Wbc54yDZ*q_PIcdQ;&lLbsS_oW$}1%{O%WtMnohk3VY5L^-AWYgh-Uwt~0xV&u9FOPMvE2"
    "Fxe~ay^tU~VvbT9^{m0B-P+dfXBSY@-TP=DY~lOdZ}g8KwRB_lEYtFnwaMHbo+<_m)>}Ri0-2X<@LLfTc?#pv<i)>!C&oqt?5VE7"
    "7eVu!Eqf|M_0~h-iCL!OQTxN-Tf>m)l!)*<`kK-$<Rcr@D@=g$m86dmI)c?0*mf(%w!rzYBYInXuqtRs*r_|%&d&uJ(;zXsj_Iyw"
    "o>sbodTI$&-s1d(JW1DFPu`F#1wmm0;7->nxss6B(&X2!vDbY>gU{5R&B<>t2$gmm;u`$cVLiuO<SKdWG-VWExIGcgYXiMw52FCz"
    "Ypql?3T1Bu>wS8nN`e3h1J&#!J%6rM(O(@Fa%C`A|1-^a-#)ma{M4R|r|GIAikZtxQ}r+Hdv@u&9{Q?5HX5$D0FlbQL*pH9@%`W{"
    "`HY>YwEn5C(eHT;4T4=)f2y_onjJIHzGqAQW&a#$%Me{J?3HFgLVsKe9AoQSo|y2uOzCCWsd`hUEjc+@+F*Y`J*Q9~hyTdRl(?H&"
    "pESxg1Tmo|I9^vAdhGk$w*!Jm-*unF75OF`+Qn$`;LrN9(N^&@s^@x`YOdD;32@t^)y#gkyRsjLaL-q+dRtl(GoCAS*6XNayzii&"
    "q|hw{;JqR6fi`Y%{#|-UC}fRWlW-P&6{c8YBTSeT>~@E0a{)T)_mlSaMn7dRdyp?U4-d=(MbQ9`;+`jO-jjA>(A5F&4yC`O)SYb&"
    ";7<I{qyMgFe|MPGTNcDtl|;KF$4k2<GO-dQxb)Ms{d}WXTh2EQ7~k@t@P3lBbp*iJ*jkK0*6icq)l;XqIR1$YoklO33LcO{15wD1"
    "x$Ez|yznH|^Wb_Fy$Wx#s_T{Au0MU`Va+x3dnJP)F6l9YH`Wk=i_HaI)B*TA-PVog(FXx4MOJ$w7!~O>dSpi+0$Inu8lNMq?{mM{"
    "0+gB-1^CoaLtZ%?YM;uJ@;6?|*}PGtWYXbr1)wS(sE<-+s;kaFB}XjNIgYvY*8m|oyRv1wQj+?hG1tL`g<nrpZckxm=%Y8jzK{wQ"
    "B%LJU8aX!rW$eQed#xg4j*c}7=D3T{(g<&U14@1OL83hMYs=G+@67ew*O?N@79WRgHuOzFLOL^twLsVh%_1`h;OOVfaM((jP2-%2"
    "LM7G~L7wf&p7UB9bKlKXSr+QL)UkBEh_FzL`NrC9+hAd+XO&!c-A2qVE}~)g>^4zF*`;Np_Lqz#C2=by$$`7EeZa-oSg?fR_Q7Uj"
    "LqyPdV(dKuUNn3(N-`p9R#EJBR`&Jp*DCfN9%Qy`XP5juu#mhrXcP%<07i(?h(3#(A_HhG1(4ssM;);$^vU;X3$#Jvpdt%_oM<2H"
    "Szgo{cg$U{d?wzSN6kj<wl8CS;zzg+Z|$mk2LRyad}Fr*g2$)oQXW#Xx4@h=ib4##>c)ymY?h;YqEcrD|1dSgSfJ5Ij4@6;Xl1qB"
    "0fvbtbuwthozG{GfUtMl+?Dn1j!0C)8x0V{*u`s}vA28;G2gN$OH-!#S80tv#D=}S5e~Z)gtvt^fF~*}IEhE{z)P8IQXWX4r?b&)"
    "QuAxiXuQj+*82CkulYt|ziW2Auj_%)*a_gF@b$Xmg8v)huJ47t_bFIuz?>f4dOGF3m|uj8&@=W07QDm<;&trh`m1}u%gV_m{$Asc"
    "`Wxj0ToY&g9lMr?YJkWO{C%u8Iu!vQoH}f<2fkNOL^_9Yms$fFVi(ak699glCH)ME`hD(l?dJj|%slkJM!8080Rf)`3wfUVgEvBF"
    "WEQ7QnbmBq3NmswJ$J08ycP?av8YCv^B@7WFw>01F?YS1Lz<s7iDbrlDH{`sa73DORgf-Z(Hs{T4qj<*R90)%(i{l(z?24d6z5LB"
    "FyH5Xd#z~}XeD~W(9ei70o(-k>a{jI2*+-H_YEYN5hG@<J5C97JejnNJ{xZ{o!Qxux&Q<bQU<LM^5--5n%<IUpzfNIsVA1CpQ4LA"
    "es|3^_@Wwn1V>ym-+F048um+@YjAm8eG7d<gfbuaYDV|dy0Q4e5xOkQirvSulbOiDPg$9^Lt*I9s67f#6HBPyMfU3T@Ql12z?XGB"
    "_*-8ImVuE(7PS{r)w1bpp}E;IvQNMt*7}2-fq{cjZCP!uU`IfsHZ?or9DqjheeUwB6GuFA=?1a+=HBBYj`=cHde2=ND+R^VVUDqK"
    "MQ|Fpo#&_=gWtVDOVvkdbRY+=ylG2&4n1DLO~GjJ>Kh>SV5JIR(pU(5$5^sC5PeE6lpJ8&>G!#>fI{OO$%7(kBK)2pM7GJOC-tNk"
    "2(o05QhAI<h?pDLbjH=`U#NQ63_T5ylU6QScWV{AxpuIXQ2UH+73AtfKh)2Z6o10cFQp0_@f;6+jUg*?HTG?Cjs0y)WeW7H?tHmj"
    "7JOsZ;?6I(Lxx1Yt)9i5@9`LT54OIiqp4nB#~2E)pFB5>82nZ;5d{{OgPvZ&_<_%A<|)zc)MrMHk8M-Kp+@YFEl5_w3U!Urehh3z"
    "u?4XFy^;xYzvUV&A;vL!&y$H8`btAn*t7-fU@#M0q?Uj==pQKcuzWt)Moc_~mCHO+jpe&XE#|(W5p3dhi7&8P_@!qEg63+|sXtLH"
    "0BJhtxpu}|?pzyhu%c_7Tw_K$i%2xjwkqLgwN0%fkJ={&m)l%~$+K_l8~0gXpLmaC2O%kz(+l;oQaQ-s^!5O;4Kfao{Sd6NfkEJ?"
    "c$xFeENa+=2afwYq$=?$$7^_we?1R=D`YJ4l;5)}%Zn{23s5rqrOz2VB_2e80vOXlSu8Oe*bGx?$J7SM0!V%t0a@!r4A3y-F_rqR"
    "R7by~_r#>}I>@ucex8niEr=*C40?STO_(t}hk^B{QdC&aBLBbB!2k7s{~s;=U}%R^2LJ"
)
#@@MANIFEST-END


def decode_embedded_man(b85: str) -> dict:
    """Inverse of the builder's make_embed_blob()."""
    d = json.loads(gzip.decompress(base64.b85decode(b85)))
    files = [{"path": p, "size": s, "sha256": h, "package": pk}
             for p, s, h, pk in d["f"]]
    return {"version": 1, "hash": d.get("h", "size"),
            "packages": d.get("pkgs", {}),
            "total_size": d.get("t", sum(f["size"] for f in files)),
            "files": files}


def embedded_manifest() -> Optional["Manifest"]:
    if not EMBEDDED_MANIFEST_B85:
        return None
    try:
        return Manifest(decode_embedded_man(EMBEDDED_MANIFEST_B85))
    except Exception as exc:  # corrupt blob → fall through to network paths
        print(f"warning: embedded manifest unreadable ({exc}); "
              "falling back to the share", file=sys.stderr)
        return None


class Manifest:
    def __init__(self, data: dict):
        self.data = data
        self.hash_algo = data.get("hash", "size")
        self.packages = data.get("packages", {})
        self.files = data.get("files", [])

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        try:
            return cls(json.loads(path.read_text()))
        except FileNotFoundError:
            sys.exit(f"Manifest not found: {path}\n"
                     "Run with --bootstrap to fetch it from the share, or "
                     "download the 'metadata' package first.")
        except json.JSONDecodeError as exc:
            sys.exit(f"Manifest is corrupt: {path}: {exc}")

    def by_package(self, pids: list[str]) -> list[dict]:
        want = set(pids)
        return [f for f in self.files if f.get("package") in want]

    def size(self, pids: list[str]) -> int:
        return sum(f["size"] for f in self.by_package(pids))


def human(n: float) -> str:
    if n < 1024:
        return f"{int(n)}B"
    for unit in ("K", "M", "G", "T"):
        n /= 1024
        if n < 1024 or unit == "T":
            return f"{n:.1f}{unit}"
    return f"{n:.1f}T"


def fmt_bar(done: int, total: int, width: int = 28) -> str:
    if total <= 0:
        return f"{human(done):>9}"
    frac = min(done / total, 1.0)
    fill = int(frac * width)
    return f"[{'#'*fill}{'.'*(width-fill)}] {frac*100:5.1f}% {human(done)}/{human(total)}"


# --------------------------------------------------------------------------
# Share URL resolution: /share/<token> -> {zone}/api/v3/onezone/shares/data/<rootId>
# --------------------------------------------------------------------------

class Share:
    """Anonymous public-share client for the DCU Onezone (Onedata >= 25).

    Real endpoints (discovered against onezone.computing.dcu.ie):
      GET  {zone}/api/v3/onezone/shares/{token}/public
             -> {"rootFileId": ..., "shareId": ..., ...}
      GET  {zone}/api/v3/onezone/shares/data/{fileId}/children
             -> {"children": [{"name","fileId"}], "nextPageToken", "isLast"}
             (POST {"token": <nextPageToken>} for the next page)
      GET  {zone}/api/v3/onezone/shares/data/{fileId}
             -> {"type": "FILE"|"DIR", "size": bytes, ...}
      GET  {zone}/api/v3/onezone/shares/data/{fileId}/content  (Range ok)
    """

    def __init__(self, url: str, subpath: str, token_override: str = ""):
        self.subpath = subpath.strip("/")
        token = token_override or self._token_from_url(url)
        if not token:
            sys.exit("Could not find a share token in the URL. Pass the full "
                     "share link, or set ASSIGN2_SHARE_TOKEN.")
        zone = self._zone_from_url(url)
        self.token = token
        self.zone = zone
        self.root_id = self._resolve_root(token)
        self._provider_cache: dict[str, str] = {}

    @staticmethod
    def _zone_from_url(url: str) -> str:
        p = urlparse(url if "://" in url else "https://" + url)
        if not p.netloc:
            sys.exit(f"Bad share URL: {url!r}")
        return f"{p.scheme}://{p.netloc}"

    @staticmethod
    def _token_from_url(url: str) -> str:
        p = urlparse(url if "://" in url else "https://" + url)
        parts = [s for s in p.path.split("/") if s]
        if len(parts) >= 2 and parts[0] == "share":
            return parts[1]
        return parts[-1] if parts else ""

    def _resolve_root(self, token: str) -> str:
        url = f"{self.zone}/api/v3/onezone/shares/{quote(token)}/public"
        # A flapping zone can serve a legacy/truncated rootFileId that every
        # data endpoint then rejects with badData (FileId: null). Probe each
        # candidate with a cheap listing before accepting it.
        last_err = "share metadata had no root file id"
        for attempt in range(5):
            j = http_json(url)
            if j is None:
                last_err = f"cannot resolve share token via {url}"
            else:
                root = next((j[k] for k in ("rootFileId", "root_file_id",
                                            "fileId", "file_id")
                             if j.get(k)), None)
                if root:
                    probe = http_json(self._children_url(root))
                    if probe is not None and not probe.get("error"):
                        return root
                    last_err = ((probe or {}).get("error", {}).get("description")
                                or "root listing failed")
            if attempt < 4:
                time.sleep(2.0 * (attempt + 1))
        sys.exit(f"Could not get a working share root: {last_err}\n"
                 "The share service may be temporarily unavailable — "
                 "retry in a few minutes.")

    def resolve_point(self, backend: Backend) -> str:
        """Walk SHARE_SUBPATH from the share root, returning the final dir id.

        A subpath that merely names the share root itself (the common case:
        the share is the corpus directory) is treated as the root, since
        children listings are relative to it already.
        """
        if not self.subpath:
            return self.root_id
        subpath = self.subpath.strip("/")
        root_name = (self.attrs(self.root_id).get("name") or "").strip("/")
        if root_name and (subpath == root_name
                          or subpath.startswith(root_name + "/")):
            subpath = subpath[len(root_name):].strip("/")
            if not subpath:
                return self.root_id
        cur = self.root_id
        for depth, part in enumerate(subpath.split("/")):
            kids = self._children(cur)
            match = [c for c in kids if c.get("name") == part]
            if not match:
                if depth == 0:
                    # Very often the share root IS the corpus dir named identically
                    # to SHARE_SUBPATH (or the zone hiccups on the attrs lookup):
                    # fall back to listing from the root itself.
                    print(f"note: no child {part!r} under share root; using "
                          "the share root itself", file=sys.stderr)
                    return self.root_id
                sys.exit(f"Subpath {self.subpath!r}: no child {part!r} in share.")
            cur = match[0]["fileId"]
        return cur

    # -- low-level API -----------------------------------------------------

    def _children_url(self, fid: str) -> str:
        return f"{self.zone}/api/v3/onezone/shares/data/{fid}/children"

    def _children(self, fid: str) -> list[dict]:
        """All children of a directory, following pagination.

        The zone intermittently answers with error payloads (serviceUnavailable,
        badData for a transiently-mismatched rootFileId). Those MUST NOT be read
        as "empty directory" — a silent 0-children report makes the downloader
        claim complete success on nothing. Retry, then bail out loudly.

        Page-size quirk (Onedata 25.1 at computing.dcu.ie, verified live): the
        unparameterised listing returns 1000 children and sets isLast=false
        with a nextPageToken even when it already returned everything, and the
        token is unredeemable (POST /children answers 405 allow: GET;
        page_token/pageToken query params are ignored; offset is capped at
        500). Passing an explicit limit=5000 returns the complete listing in
        one page with an honest isLast — DCU Assignment-2 directories never
        exceed 1000 entries, so that single page is always enough.
        """
        out: list[dict] = []
        token: Optional[str] = None
        for _ in range(10000):
            j = None
            for attempt in range(6):
                if token:
                    j = http_json(self._children_url(fid), method="POST",
                                  body={"token": token})
                else:
                    j = http_json(self._children_url(fid) + "?limit=5000")
                if j is not None and not j.get("error"):
                    break
                if attempt < 5:
                    time.sleep(1.5 * (attempt + 1))
            if j is None or j.get("error"):
                desc = (j.get("error", {}).get("description", "")
                        if isinstance(j, dict) else "")
                how = ("continuation POST" if token else "listing")
                sys.exit(f"Share {how} for {fid!r} keeps failing: {desc or j!r}\n"
                         "The share service may be temporarily unavailable — "
                         "retry in a few minutes.")
            out.extend(j.get("children", []))
            token = j.get("nextPageToken") or j.get("next_page_token")
            if j.get("isLast"):
                token = None
            if j.get("isLast") or not token:
                break
        return out

    def attrs(self, fid: str) -> dict:
        url = f"{self.zone}/api/v3/onezone/shares/data/{fid}"
        for attempt in range(5):
            j = http_json(url) or {}
            if not j.get("error"):
                return j
            if attempt < 4:
                time.sleep(1.5 * (attempt + 1))
        return j

    def content_url(self, fid: str) -> str:
        """Onezone content URL. It 307-redirects to an Oneprovider.

        We resolve the redirect once and Range against the provider directly:
        the Onezone loses the Range header across its redirect, the Oneprovider
        honours it (206 + Content-Range).
        """
        return f"{self.zone}/api/v3/onezone/shares/data/{fid}/content"

    def provider_content_url(self, fid: str) -> str:
        """Resolve the provider URL for a file, caching per file id."""
        if fid in self._provider_cache:
            return self._provider_cache[fid]
        url = self.content_url(fid)
        direct = _resolve_redirect(url)
        self._provider_cache[fid] = direct
        return direct

    def attrs_many(self, fids: list[str], workers: int = 12) -> dict[str, dict]:
        """attrs() for many ids concurrently.

        Listings are cheap (one call per directory) but the per-entry attrs
        call dominates: ~1.5 s each, so a 5000-file share takes minutes on one
        thread. These run against one host, so 12 workers is polite.
        """
        if not fids:
            return {}
        if len(fids) == 1:
            return {fids[0]: self.attrs(fids[0])}
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(workers, len(fids))) as pool:
            results = list(pool.map(self.attrs, fids))
        return dict(zip(fids, results))

    def walk(self, root_fid: str, on_dir=None, workers: int = 12) -> dict[str, dict]:
        """Depth-first walk; returns {relpath: {id, size}} for every file.

        Listings omit type/size, so each entry is resolved with one attrs call;
        those calls are issued in parallel per directory level.
        """
        out: dict[str, dict] = {}
        stack = [("", root_fid)]
        while stack:
            rel, fid = stack.pop()
            kids = self._children(fid)
            if on_dir:
                on_dir(rel or "/", len(kids))
            ids, names = [], []
            for c in kids:
                cid = c.get("fileId") or c.get("file_id") or c.get("id")
                name = c.get("name", "")
                if cid and name:
                    ids.append(cid)
                    names.append(name)
            if not ids:
                continue
            attrs = self.attrs_many(ids, workers)
            for cid, name in zip(ids, names):
                path = f"{rel}/{name}".strip("/")
                a = attrs.get(cid) or {}
                if a.get("type") == "DIR":
                    stack.append((path, cid))
                else:
                    out[path] = {"id": cid, "size": a.get("size", 0)}
        return out

    def resolve_paths(self, root_fid: str, relpaths) -> dict[str, dict]:
        """Resolve wanted relpaths to fileIds via directory listings only.

        Used when a manifest (embedded or --manifest) is present: it already
        carries sizes/hashes, so no per-entry attrs call is needed — one
        `children` call per directory that actually holds a wanted file.
        Selected files that are not uploaded yet are simply absent.
        """
        by_dir: dict[str, list[tuple[str, str]]] = {}
        for p in dict.fromkeys(relpaths):
            d, _, name = p.rpartition("/")
            by_dir.setdefault(d, []).append((name, p))
        out: dict[str, dict] = {}
        pending = {d for d in by_dir if d}
        queue = [("", root_fid)]
        while queue:
            rel, fid = queue.pop(0)
            kids: dict[str, str] = {}
            for c in self._children(fid):
                cid = c.get("fileId") or c.get("file_id") or c.get("id")
                if cid and c.get("name"):
                    kids[c["name"]] = cid
            for name, full in by_dir.get(rel, []):
                if name in kids:
                    out[full] = {"id": kids[name]}
            for d in list(pending):
                parent, _, leaf = d.rpartition("/")
                if parent == rel and leaf in kids:
                    queue.append((d, kids[leaf]))
                    pending.discard(d)
        return out


_JSON_BACKEND: Optional[Backend] = None
_JSON_CURL = shutil.which("curl")


def http_json(url: str, method: str = "GET", body: Optional[dict] = None,
              retries: int = 3):
    """Small JSON GET/POST — uses curl if present, else requests, else urllib.

    Retries transient failures with backoff so one flaky listing call does not
    abort a long walk.
    """
    delay = 1.0
    for attempt in range(1, retries + 1):
        res = _http_json_once(url, method, body)
        if res is not None:
            return res
        if attempt < retries:
            time.sleep(delay)
            delay = min(delay * 2, 15)
    return None


def _http_json_once(url: str, method: str, body: Optional[dict]):
    if _JSON_CURL:
        args = [_JSON_CURL, "-sS", "-L", "-A", USER_AGENT, "-X", method, url]
        if body is not None:
            args += ["-H", "Content-Type: application/json",
                     "--data-binary", json.dumps(body)]
        r = subprocess.run(args, capture_output=True, text=True, timeout=120)
        if r.returncode != 0 or not r.stdout.strip():
            return None
        try:
            return json.loads(r.stdout)
        except json.JSONDecodeError:
            return None
    try:
        import urllib.request
        req = urllib.request.Request(url, method=method,
                                     headers={"User-Agent": USER_AGENT})
        if body is not None:
            req.add_header("Content-Type", "application/json")
            req.data = json.dumps(body).encode()
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except Exception:  # noqa: BLE001
        return None


# --------------------------------------------------------------------------
# Download engine
# --------------------------------------------------------------------------

class Downloader:
    def __init__(self, share: Share, backend: Backend, manifest: Manifest,
                 dest: Path):
        self.share = share
        self.backend = backend
        self.manifest = manifest
        self.dest = dest
        self.session = {"ok": 0, "fail": 0, "skip": 0, "bytes": 0, "missing": 0}
        self.failures: list[str] = []
        self._interrupted = False
        self._lock = threading.Lock()

    # -- counters (thread-safe) -------------------------------------------

    def _bump(self, key: str, n: int = 1) -> None:
        with self._lock:
            self.session[key] += n

    def _record_failure(self, msg: str) -> None:
        with self._lock:
            self.failures.append(msg)

    # -- integrity ---------------------------------------------------------

    def _verify(self, path: Path, entry: dict) -> tuple[bool, str]:
        if not path.exists():
            return False, "missing"
        real = path.stat().st_size
        if real != entry["size"]:
            return False, f"size {real} != {entry['size']}"
        if self.manifest.hash_algo == "sha256" and entry.get("sha256"):
            h = hashlib.sha256()
            with path.open("rb") as fh:
                while chunk := fh.read(1 << 20):
                    h.update(chunk)
            if h.hexdigest() != entry["sha256"]:
                return False, "sha256 mismatch"
        return True, ""

    # -- one file ----------------------------------------------------------

    def _download_one(self, entry: dict, file_id: str, label: str,
                      retries: int) -> bool:
        target = self.dest / entry["path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        part = target.with_name(target.name + ".part")

        if target.exists():
            ok, why = self._verify(target, entry)
            if ok:
                self._bump("skip")
                return True
            print(f"  ~ {label}: exists but {why} — refetching")
            target.unlink()

        url = self.share.provider_content_url(file_id)
        total = entry["size"]
        delay = 2.0

        for attempt in range(1, retries + 1):
            offset = part.stat().st_size if part.exists() else 0
            if offset > total:          # stale oversized part
                part.unlink()
                offset = 0
            if offset == total:
                break

            last = [offset]
            def prog(done, _tot, _last=last):
                _last[0] = done

            status, err = self.backend.get_stream(url, offset, part, 0,
                                                  lambda d, t: prog(d, t))
            if err is None and status == 200 and offset > 0:
                # provider ignored Range; curl appended onto the old prefix
                if not part.exists() or part.stat().st_size != total:
                    part.unlink(missing_ok=True)
                    if attempt < retries:
                        continue
                    
            if err is None and part.exists() and part.stat().st_size == total:
                break
            if part.exists() and part.stat().st_size == total:
                break
            if err:
                if attempt < retries:
                    print(f"  ! {label}: {err} — retry {attempt}/{retries-1} "
                          f"in {delay:.0f}s")
                    time.sleep(delay)
                    delay = min(delay * 2, 60)
                else:
                    return False
            else:
                if attempt < retries:
                    time.sleep(delay)
                    delay = min(delay * 2, 60)
                else:
                    return False

        if not part.exists() or part.stat().st_size != total:
            got = part.stat().st_size if part.exists() else 0
            self._record_failure(f"{entry['path']} (got {got}/{total})")
            return False

        ok, why = self._verify(part, entry)
        if not ok:
            self._record_failure(f"{entry['path']} ({why})")
            part.unlink()
            return False

        os.replace(part, target)
        self._bump("ok")
        self._bump("bytes", total)
        return True

    # -- many files --------------------------------------------------------

    def run(self, entries: list[dict], filemap: dict[str, dict],
            retries: int, jobs: int) -> int:
        if jobs and jobs > 1:
            return self._run_parallel(entries, filemap, retries, jobs)
        ln = len(entries)
        for i, e in enumerate(entries, 1):
            fid = filemap.get(e["path"], {}).get("id")
            label = f"[{i}/{ln}] {e['path']}"
            print(f"{label}  ")
            if fid is None:
                self._bump("missing")
                print(f"  - not uploaded yet")
                continue
            ok = self._download_one(e, fid, label, retries)
            if ok:
                print(f"  ok {human(e['size'])}")
        return self.session["fail"]

    def _run_parallel(self, entries: list[dict], filemap: dict[str, dict],
                      retries: int, jobs: int) -> int:
        """Same semantics as run(), `jobs` files at a time.

        Each file owns its .part, its own target dir mkdir is idempotent, and
        the counters are guarded by a lock, so this is safe without more.
        """
        import threading
        lock = self._lock
        pending = [e for e in entries if e["path"] in filemap]
        missing = sum(1 for e in entries if e["path"] not in filemap)
        if missing:
            self._bump("missing", missing)
        total = len(entries)
        done = [total - len(pending)]

        def work(e: dict):
            fid = filemap[e["path"]]["id"]
            label = e["path"]
            ok = self._download_one(e, fid, label, retries)
            with lock:
                done[0] += 1
                print(f"[{done[0]}/{total}] {label}  "
                      f"{'ok' if ok else 'FAIL'} {human(e['size'])}")

        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=jobs) as pool:
            list(pool.map(work, pending))
        with lock:
            self.session["fail"] = len(self.failures)
        return self.session["fail"]

    def summary(self) -> int:
        s = self.session
        print("\n" + "=" * 60)
        print(f"downloaded {s['ok']}  already-present {s['skip']}  "
              f"failed {len(self.failures)}  "
              f"not-yet-uploaded {s['missing']}")
        if self.failures:
            print("\nFailures (re-run to retry):")
            for f in self.failures[:40]:
                print("  -", f)
            if len(self.failures) > 40:
                print(f"  … and {len(self.failures)-40} more")
            return 2
        if s["missing"]:
            print("Note: some files are not on the share yet; re-run later to "
                  "pick them up.")
        return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def parse_packages(arg: str, known: list[str]) -> list[str]:
    if arg.strip().lower() == "all":
        return known
    out = []
    for p in arg.split(","):
        p = p.strip()
        if p and p not in out:
            out.append(p)
    return out


def parse_video_set(spec: str) -> set:
    """Parse a --videos spec like "31-40,55,00077" into a set of ids."""
    if not spec or not spec.strip():
        sys.exit("--videos: empty specification (expected e.g. 31-40,55)")
    ids = set()
    for tok in spec.replace(" ", "").split(","):
        if not tok:
            continue
        if "-" in tok:
            lo, _, hi = tok.partition("-")
            if not (lo.isdigit() and hi.isdigit()):
                sys.exit(f"--videos: bad range {tok!r} (expected e.g. 31-40)")
            lo, hi = int(lo), int(hi)
            if lo > hi:
                lo, hi = hi, lo
            ids.update(range(lo, hi + 1))
        elif tok.isdigit():
            ids.add(int(tok))
        else:
            sys.exit(f"--videos: bad id {tok!r} (expected digits, optionally "
                     "as a-b ranges; e.g. 31-40,55)")
    return ids


def filter_videos(entries: list, ids: set, explicit: set) -> tuple:
    """Narrow a manifest selection to whole videos by their numeric id.

    A file belongs to video N when its name without extension is the
    (zero-padded) video id, e.g. "videos_480p/00031.mp4" or
    "info/00031.json". Files that are global to the corpus cannot be
    subset: the videos_metadata.* index is always kept (it is what maps
    ids to anything useful), while any other global file (e.g. msb.zip)
    is kept only when its package was explicitly requested via
    --packages/--all. Returns (kept_entries, matched_ids).
    """
    kept, matched = [], set()
    for e in entries:
        stem = Path(e["path"]).stem
        if stem.isdigit():
            n = int(stem)
            if n in ids:
                kept.append(e)
                matched.add(n)
        elif Path(e["path"]).name.startswith("videos_metadata."):
            kept.append(e)
        elif e.get("package") in explicit:
            kept.append(e)
    return kept, matched


def interactive_select(manifest: Manifest) -> list[str]:
    pkgs = manifest.packages
    ids = list(pkgs)
    print("\nAvailable packages:\n")
    for i, pid in enumerate(ids, 1):
        p = pkgs[pid]
        tag = " *recommended*" if p.get("recommended") else ""
        print(f"  [{i}] {p['title']}{tag}")
        print(f"      {p['description']}")
        print(f"      {human(p['size'])}, {p['files']} files")
        print()
    print(f"  [a] everything ({human(manifest.data['total_size'])})")
    print("  Comma-separated numbers/ids, e.g. `1,3` or `metadata,msb`.\n")
    try:
        raw = input("Your selection: ").strip()
    except EOFError:
        print("(no interactive terminal; pass --packages <ids> or --all instead)")
        raise SystemExit(2)
    except KeyboardInterrupt:
        print("\nCancelled.")
        raise SystemExit(130)
    if raw.lower() in ("a", "all", "*"):
        return ids
    chosen = []
    for tok in raw.replace(" ", ",").split(","):
        if not tok:
            continue
        if tok.isdigit() and 1 <= int(tok) <= len(ids):
            chosen.append(ids[int(tok) - 1])
        elif tok in pkgs:
            chosen.append(tok)
        else:
            print(f"  ? unknown package {tok!r}")
    return chosen


def check_space(dest: Path, need: int) -> bool:
    dest.mkdir(parents=True, exist_ok=True)
    try:
        free = shutil.disk_usage(dest).free
    except OSError:
        return True
    if free < need:
        print(f"\nWARNING: need {human(need)} but only {human(free)} free at "
              f"{dest}.")
        try:
            ans = input("Continue anyway? [y/N] ").strip().lower()
        except EOFError:            # --jobs / cron / pipe: assume yes
            print("(no tty; continuing)")
            return True
        return ans in ("y", "yes")
    print(f"Disk check: {human(free)} free, {human(need)} needed.")
    return True


def _resolve_redirect(url: str) -> str:
    """Follow a 307 (onezone -> oneprovider) and return the target URL.

    Falls back to the input URL if resolution fails, so a provider that answers
    directly still works.
    """
    if _JSON_CURL:
        r = subprocess.run([_JSON_CURL, "-sS", "-o", "/dev/null", "-w",
                            "%{redirect_url}", "--max-redirs", "0", url],
                           capture_output=True, text=True, timeout=60)
        target = r.stdout.strip()
        if target:
            return target
    try:
        import urllib.request
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                return None
        op = urllib.request.build_opener(NoRedirect)
        try:
            op.open(urllib.request.Request(url, method="GET",
                    headers={"User-Agent": USER_AGENT}))
        except urllib.error.HTTPError as e:
            if e.headers.get("Location"):
                return e.headers["Location"]
    except Exception:  # noqa: BLE001
        pass
    return url


def bootstrap(share_url: str, dest: Path) -> int:
    """Fetch download_manifest.json + download_packages.json from the share.

    Used when a student has only this script (e.g. copied from the handout) and
    needs the manifest before it can select packages. The manifest is small, so
    a shallow targeted walk is enough.
    """
    dest.mkdir(parents=True, exist_ok=True)
    share = Share(share_url, SHARE_SUBPATH, SHARE_TOKEN)
    backend = pick_backend()
    print(f"Share zone: {share.zone}")
    root = share.resolve_point(backend)

    wanted = {"download_manifest.json", "download_packages.json"}
    found: dict[str, str] = {}
    stack = [("", root)]
    while stack and len(found) < len(wanted):
        rel, fid = stack.pop()
        for c in share._children(fid):
            name = c.get("name", "")
            cid = c.get("fileId") or c.get("file_id") or c.get("id")
            if name in wanted and name not in found:
                found[name] = cid
                continue
            # descend a couple of levels looking for the manifest
            if rel.count("/") < 3:
                a = share.attrs(cid or "")
                if a.get("type") == "DIR":
                    stack.append((f"{rel}/{name}".strip("/"), cid))

    if not found:
        print("No download_manifest.json on the share yet; "
              "synthesizing one from the live listing…")
        return bootstrap_synthesize(share, backend, root, dest)

    for name, fid in found.items():
        target = dest / name
        print(f"  fetching {name} -> {target}")
        status, err = backend.get_stream(share.content_url(fid), 0, target, 0,
                                         lambda *_: None)
        if err:
            sys.exit(f"Failed to fetch {name}: {err}")
    print(f"\nManifest ready. Now run:\n  python3 {Path(__file__).name} "
          f"--dest {dest}")
    return 0


def bootstrap_synthesize(share, backend, root, dest: Path) -> int:
    """Build a size-only manifest from whatever is currently on the share.

    Used during the upload window before the real manifest exists. Package ids
    are inferred from the top-level directories/files.
    """
    filemap = share.walk(root)
    pkg_of = {
        "info": "metadata", "videos_metadata.csv": "metadata",
        "videos_metadata.json": "metadata", "videos_metadata.md": "metadata",
        "msb.zip": "msb", "thumbnails": "thumbnails",
        "videos_480p": "videos_480p", "keyframes": "keyframes",
        "videos_original": "videos_original",
    }
    files = []
    for path, info in sorted(filemap.items()):
        top = path.split("/")[0]
        pkg = pkg_of.get(top)
        if not pkg:
            continue
        files.append({"path": path, "size": info.get("size", 0),
                      "package": pkg})

    man = {"version": 1, "hash": "size",
           "generated": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
           "packages": {}, "files": files,
           "total_size": sum(f["size"] for f in files)}
    for pid in sorted({f["package"] for f in files}):
        fl = [f for f in files if f["package"] == pid]
        man["packages"][pid] = {
            "title": pid, "description": "(from live listing)",
            "size": sum(f["size"] for f in fl), "files": len(fl),
            "recommended": pid in ("metadata", "msb"),
        }

    (dest / "download_manifest.json").write_text(json.dumps(man, indent=1))
    (dest / "download_packages.json").write_text(
        json.dumps({"version": 1, "packages": man["packages"],
                    "total_size": man["total_size"]}, indent=1))
    print(f"  synthesized manifest: {len(files)} files, "
          f"{human(man['total_size'])}")
    print(f"  (size-only; sha256 validation needs the published manifest)")
    print(f"\nNow run:\n  python3 {Path(__file__).name} --dest {dest}")
    return 0


def probe(entries: list[dict], filemap: dict[str, dict],
          missing: list[str]) -> int:
    """Report upload coverage per package without downloading."""
    import collections
    stats: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0, 0])
    for e in entries:
        pid = e.get("package", "?")
        remote = filemap.get(e["path"])
        if remote is None:
            stats[pid][1] += 1
            stats[pid][2] += e["size"]
        elif remote.get("size") and remote["size"] != e["size"]:
            stats[pid][1] += 1
            stats[pid][2] += e["size"]
        else:
            stats[pid][0] += 1
    print("\nUpload coverage (present / total, missing bytes):")
    for pid, (ok, bad, badbytes) in stats.items():
        tot = ok + bad
        flag = "COMPLETE" if bad == 0 else "UPLOADING"
        print(f"  {pid:16} {ok:5}/{tot:<5} {flag:10} {human(badbytes)} missing")
    return 0


def cmd_verify(manifest: Manifest, dest: Path) -> int:
    bad = 0
    for e in manifest.files:
        p = dest / e["path"]
        if not p.exists():
            continue
        if p.stat().st_size != e["size"]:
            print(f"BAD size  {e['path']}")
            bad += 1
        elif manifest.hash_algo == "sha256" and e.get("sha256"):
            h = hashlib.sha256()
            with p.open("rb") as fh:
                while chunk := fh.read(1 << 20):
                    h.update(chunk)
            if h.hexdigest() != e["sha256"]:
                print(f"BAD hash  {e['path']}")
                bad += 1
    print(f"Verification complete: {bad} bad file(s).")
    return 3 if bad else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Download the Assignment 2 corpus from the DCU Onedata share.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""examples:
  python3 student_download.py                     interactive menu
  python3 student_download.py --list              packages and sizes
  python3 student_download.py --list --videos 1-99    preview a batch (offline)
  python3 student_download.py --packages metadata smallest essential set
  python3 student_download.py --packages metadata,msb,thumbnails,videos_480p
  python3 student_download.py --videos 31-40,55   only those whole videos
  python3 student_download.py --videos 100-199 --packages videos_480p

Re-run the same command to resume; verified files are skipped.  With
--videos, files carry the video id used by videos_metadata.csv: 31 means
00031, i.e. videos_480p/00031.mp4, keyframes/00031.zip, info/00031.json
and thumbnails/00031.zip (videos_metadata.* always comes along).  Sample
ids are sparse, so a range can match fewer videos than its width suggests —
run --list --videos IDS first to preview a batch.  Ranges may be reversed
(100-1 means 1-100).  Sizes shown on selection reflect the subset.""")
    ap.add_argument("--packages", "-p", metavar="LIST",
                    help="comma-separated package ids, or 'all'")
    ap.add_argument("--all", action="store_true", help="download everything")
    ap.add_argument("--videos", metavar="IDS", default=None,
                    help="download only whole videos whose id matches IDS: "
                         "a comma list of ids and/or a-b ranges, e.g. "
                         "31-40,55 (ids are the numeric file names, see "
                         "videos_metadata.csv; a-b is inclusive and may be "
                         "reversed, 40-31 means 31-40). Applies after "
                         "--packages/--all; slices every package on its "
                         "own, always keeps videos_metadata.*, and skips "
                         "msb.zip (a zip cannot be subset) unless its "
                         "package is explicitly selected")
    ap.add_argument("--dest", "-d", default=DEFAULT_DEST,
                    help=f"destination directory (default: {DEFAULT_DEST})")
    ap.add_argument("--manifest", default=None,
                    help="path to download_manifest.json (default: <dest>/…)")
    ap.add_argument("--jobs", "-j", type=int, default=8,
                    help="parallel file downloads (default 8)")
    ap.add_argument("--walk-workers", type=int, default=12, metavar="N",
                    help="parallel share-listing lookups (default 12); the "
                         "listing itself is the slow part on a big share")
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument("--backend", choices=["curl", "wget", "requests"])
    ap.add_argument("--verify-only", action="store_true")
    ap.add_argument("--list", action="store_true",
                    help="list packages and exit")
    ap.add_argument("--probe", action="store_true",
                    help="report which selected files are uploaded to the share "
                         "yet (no download)")
    ap.add_argument("--bootstrap", action="store_true",
                    help="fetch just the manifest from the share into --dest "
                         "(use when you only have this script)")
    ap.add_argument("--share", default=None, help="override share URL")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)

    dest = Path(a.dest).expanduser().resolve()

    if a.bootstrap:
        return bootstrap(a.share or SHARE_URL, dest)

    # manifest precedence: --manifest file > local copy > embedded index
    if a.manifest:
        manifest, source = Manifest.load(Path(a.manifest)), f"file {a.manifest}"
    else:
        for cand in (dest / "download_manifest.json",
                     Path(__file__).resolve().parent / "download_manifest.json",
                     Path(__file__).resolve().parent / "V3C1" / "sample"
                     / "download_manifest.json"):
            if cand.is_file():
                manifest, source = Manifest.load(cand), f"file {cand}"
                break
        else:
            manifest = embedded_manifest()
            if manifest is None:
                sys.exit(
                    "No download manifest available: this copy carries none "
                    "embedded, none was found next to the script or in --dest. "
                    "Re-run with --bootstrap to build one from the share, or "
                    "use the script distributed with the handout.")
            source = "embedded index"
    print(f"Manifest: {source}")

    if a.list:
        for pid, p in manifest.packages.items():
            print(f"{pid:16} {human(p['size']):>10} {p['files']:>6} files  "
                  f"{p['title']}")
        print(f"{'TOTAL':16} {human(manifest.data['total_size']):>10} "
              f"{len(manifest.files):>6} files")
        if a.videos:
            vset = parse_video_set(a.videos)
            sub, _m = filter_videos(manifest.files, vset, set())
            print(f"\n--videos {a.videos}: {len(sub)} files, "
                  f"{human(sum(e['size'] for e in sub))}; first entries:")
            for e in sub[:8]:
                print(f"  {human(e['size']):>9}  {e['path']}")
            if len(sub) > 8:
                print(f"  … and {len(sub) - 8} more")
        return 0

    if a.verify_only:
        return cmd_verify(manifest, dest)

    if a.all:
        selected = list(manifest.packages)
    elif a.packages:
        selected = parse_packages(a.packages, list(manifest.packages))
    elif a.probe and os.environ.get("ASSIGN2_PACKAGES"):
        selected = parse_packages(os.environ["ASSIGN2_PACKAGES"],
                                  list(manifest.packages))
    elif a.videos:
        selected = list(manifest.packages)
    else:
        if not sys.stdin.isatty():
            print("No interactive terminal detected — use --list to see the "
                  "packages, then pass --packages <ids> or --all.")
            print("Available packages: " + ", ".join(manifest.packages))
            return 2
        selected = interactive_select(manifest)
    if not selected:
        print("Nothing selected.")
        return 0
    unknown = [p for p in selected if p not in manifest.packages]
    if unknown:
        sys.exit(f"Unknown package(s): {', '.join(unknown)}")

    entries = manifest.by_package(selected)
    vset_note = ""
    if a.videos:
        vset = parse_video_set(a.videos)
        # explicit only when the user picked packages themselves; a bare
        # --videos run slices every package but must not drag msb.zip in
        explicit = set(selected) if (a.all or a.packages) else set()
        entries, matched = filter_videos(entries, vset, explicit)
        if not entries:
            sys.exit(f"No files match --videos {a.videos!r} in package(s) "
                     f"{', '.join(selected)}; check ids against "
                     "videos_metadata.csv.")
        unmatched = sorted(vset - matched)
        if unmatched:
            preview = ", ".join(map(str, unmatched[:20]))
            more = (f" … (+{len(unmatched) - 20} more)"
                    if len(unmatched) > 20 else "")
            print(f"WARNING: {len(unmatched)} requested video id(s) matched "
                  f"no manifest file: {preview}{more}")
        vset_note = f", videos {a.videos}"
    need = sum(e["size"] for e in entries)
    print(f"\nSelected: {', '.join(selected)}{vset_note}")
    print(f"{len(entries)} files, {human(need)}")

    try:
        share = Share(a.share or SHARE_URL, SHARE_SUBPATH, SHARE_TOKEN)
    except SystemExit as exc:
        if a.dry_run:
            print(f"(share unreachable: {exc})")
            return 0
        raise
    backend = pick_backend(a.backend)
    print(f"Share zone: {share.zone}  root: {share.root_id}  backend: {backend.name}")

    if a.dry_run:
        return 0

    print("Resolving selected files on the share (directory listings)…")
    root_point = share.resolve_point(backend)
    filemap = share.resolve_paths(root_point, (e["path"] for e in entries))
    missing = [e["path"] for e in entries if e["path"] not in filemap]
    print(f"Found {len(entries) - len(missing)}/{len(entries)} selected "
          f"files on the share.")

    if a.probe:
        return probe(entries, filemap, missing)

    if not check_space(dest, need):
        return 0

    if missing:
        print(f"WARNING: {len(missing)} selected file(s) not uploaded yet "
              f"(will be skipped):")
        for m in missing[:10]:
            print("  -", m)
        if len(missing) > 10:
            print(f"  … and {len(missing)-10} more")

    dl = Downloader(share, backend, manifest, dest)

    def on_sigint(_sig, _frm):
        dl._interrupted = True
    signal.signal(signal.SIGINT, on_sigint)

    try:
        dl.run(entries, filemap, a.retries, a.jobs)
    except KeyboardInterrupt:
        print("\nInterrupted — partial files kept; re-run to resume.")
    return dl.summary()


if __name__ == "__main__":
    sys.exit(main())