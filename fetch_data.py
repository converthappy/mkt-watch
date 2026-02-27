#!/usr/bin/env python3
"""
Fetch sector relative strength data via yfinance.
Outputs columnar JSON files to data/ directory for use by dashboard.html.

Usage:
  python3 fetch_data.py                  # full 5-year refresh (default)
  python3 fetch_data.py --mode full      # same as above
  python3 fetch_data.py --mode incremental  # append only new trading days
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta

try:
    import yfinance as yf
except ImportError:
    print("yfinance not installed. Run: pip3 install yfinance")
    sys.exit(1)

# Mirror of the 16 panels from dashboard.html
PANELS = [
    {
        "title": "1. Market ETFs Overview",
        "baseSymbol": "SPY",
        "symbols": ["SPY","RSP","QQQ","DIA","IWM","XLK","RYT","XLV","RYH","XLF","RYF","XLY","RCD","XLP","RHS","XLE","RYE","XLI","RGI","XLB","RTM","XLC","RSPC","XLU","RYU","XLRE","EWRE"]
    },
    {
        "title": "2. Tech - Software & Services",
        "baseSymbol": "XLK",
        "symbols": ["XLK","RYT","FSLY","RNG","VIAV","DOCN","VNET","KC","LLYVA","LLYVK","WULF","GDS","ZM","IDCC","NTCT","NN","META","NSIT","FFIV","RXO","MRX","FTNT","NICE","AUR","AKAM","TDC","SLDE","DLB","MTCH","INGM","BILL","RUM","ETOR","J","NET","DBX","HNGE","CDW","DLO","YOU","GOOGL","GOOG","ADEA","KBR","DDOG","NBIS","SPOT","CDNS","TRMB","ACIW","PTC","IAC","SRAD","NFLX","S","OPLN","STRK","PLUS","BILI","TWLO","FIG","EQPT","SEZL","NTNX","TME","LIF","CRWV","HPQ","CORZ","CACI","GRND","ALRM","XMTR","ATHM","PSN","DT","TENB","HUT","RBA","DXC","LDOS","BLKB","BB","CARG","CHKP","ADSK","MDB","ADBE","GEN","BSY","BOX","PEGA","IBM","PONY","TEM","SEI","CRWD","GDDY","VRSN","WIT","MANH","ROKU","CRCL","SNPS","RIOT","OTEX","BIDU","MBLY","WIX","PAYX","JKHY","DOX","SSNC","GIB","AVPT","BR","OKTA","CRM","WAY","IOT","PL","VRSK","ESTC","SNOW","ROP","TTWO","VEEV","SAIC","ORCL","CNXC","GWRE","SAIL","BLSH","APPF","NOW","TOST","MSTR","ZETA","PCOR","CRDO","FIS","COMP","PANW","WRD","CIFR","CLBT","PLTR","INFY","DOCU","QTWO","GTLB","DSGX","RBLX","CTSH","ACN","CSGP","ZS","CVLT","RBRK","PAYC","HUBS","PCTY","DUOL","CLSK","MARA","BULL","APP","WDAY","MMYT","PATH","HTFL","SOUN","TYL","TTAN","AGYS","WK","TTD","VRNS","NAVN","EXLS","GLOB","INTU","KVYO","QLYS","NTSK","BL","SNAP","PINS","IT","TRI","ZG","FRSH","Z","RDDT","VERX","EPAM","CCC","SPSC","STUB","TEAM","NIQ","DOCS","MNDY","KD","U"]
    },
    {
        "title": "3. Tech - Hardware & Semis",
        "baseSymbol": "SMH",
        "symbols": ["SMH","SOXX","IPGP","GLW","CGNX","TER","SNDK","CIEN","VRT","UI","ENPH","AAOI","AEIS","LFUS","ASX","COHR","WDC","ESE","GRMN","STX","NXT","STM","LASR","RUN","MOG.A","ADI","DIOD","FORM","DBD","Q","BHE","HWM","LSCC","BELFB","BELFA","ALGM","SIMO","MPWR","MU","TXN","KN","KEYS","ARM","ON","FN","AIR","NATL","CRUS","TSM","PLXS","ST","SMTC","ASML","NOVT","SITM","FTV","VSAT","DELL","TTMI","CW","VSH","PLAB","NTAP","MTSI","CSCO","VICR","NVDA","NVT","ZBRA","PSTG","ATRO","MCHP","SXI","VISN","POWI","HII","GFS","HPE","ANET","SWKS","FTAI","ESLT","TEL","MCHPP","SMCI","CAMT","QRVO","ITRI","AMBA","EMR","MTRN","FLEX","OSIS","ONTO","KLAC","AVGO","BWXT","NXPI","FSLR","MRVL","APH","TSEM","CALX","SYNA","NVMI","BE","AMKR","DRS","CLS","BA","LOAR","MRCY","AMTM","OLED","INTC","RMBS","AMD","SANM","RAL","PI","LUNR","MIR","PLUG","ACHR","RKLB","AVAV","KRMN","KTOS","ONDS","ALAB","AXON","JOBY","BETA","QBTS","FLY","RGTI","IONQ"]
    },
    {
        "title": "4. Financials",
        "baseSymbol": "XLF",
        "symbols": ["XLF","RYF","MBIN","CASH","XP","SNEX","IRM","BBDO","SII","BBT","TW","FFBC","BBD","HG","UNIT","VLY","VCTR","CACC","EBC","BEN","BSBR","INDB","HASI","MPT","FLG","RNR","MAC","IFS","INTR","VIRT","SPNT","BFH","ACT","LU","BGC","WAL","WSC","TCBI","HGV","BAP","MCHB","GBCI","LINE","OUT","SMA","MKTX","JXN","CBC","AX","MTH","ZION","IBKR","NMIH","MGRC","PNFP","WFC","C","CIB","FFIN","NU","VOYA","STT","WU","CCI","BN","BMA","COLD","GCMG","PRI","WD","CRBG","LNC","BUR","KYIV","FNF","BBAR","BAM","AB","URI","AVAL","HRI","PK","BANC","GS","BK","BLK","LAZ","FRHC","CNS","GGAL","LXP","SEIC","EQH","MCY","APAM","FRMI","MS","CUBI","FSV","AON","ERIE","AMP","FCNCA","DAVE","FG","PRVA","KSPI","RJF","SF","MIAX","ENVA","HQY","WTW","SCHW","ARE","PLMR","HGTY","IVZ","BXP","UHAL.B","KNSL","OBDC","UHAL","TROW","COF","CUZ","HHH","FUTU","JLL","ICE","OMF","AMG","EVR","VNO","PIPR","HLI","CBRE","BRO","APO/PA","NMRK","KRC","JEF","LPLA","SLM","TBBK","HIW","KKR/PD","APO","CG","AJG","OTF","NDAQ","MC","NP","HTGC","RKT","PAX","KKR","CHYM","UWMC","SLG","PJT","ARES/PB","CAR","RYAN","CIGI","CWK","BX","OPEN","COIN","ARES","SOFI","ARX","AFRM","XXI","LMND","HOOD","HLNE","CRVL","BMNR","TPG","OWL","STEP","GLXY","UPST","PFSI","BWIN"]
    },
    {
        "title": "5. Producer Manufacturing",
        "baseSymbol": "XLI",
        "symbols": ["XLI","RGI","LITE","MOD","RRX","GNRC","UCTT","ACMR","POWL","BDC","OII","CYD","BWA","AGCO","DAN","KLIC","CNH","GTES","GEV","MKSI","CAT","WWD","CECO","TKR","EFXT","SEDG","AMAT","KMT","ATS","CSL","NPO","FLS","ATMU","CARR","NWL","TEX","ITT","ENTG","HLIO","LEA","PSIX","LECO","PHIN","SPXC","IR","HUBB","LII","ETN","LRCX","AAON","JBL","WMS","WSO","ESAB","ALH","GFF","LCII","KAI","BRC","GTX","MHK","BC","CMI","APTV","ENS","CXT","VC","AYI","TGLS","YETI","ROK","HSAI","PNR","BMI","ATKR","VLTO","FBIN","MLI","ACLS","SYM","SMR","OKLO","QS","EOSE"]
    },
    {
        "title": "6. Non-Energy Minerals (Materials)",
        "baseSymbol": "XLB",
        "symbols": ["XLB","RTM","ALM","ATI","SSRM","AG","SKE","KNF","CRS","CSTM","AGI","AUGO","FSM","TECK","STLD","TTAM","TGB","WS","SCCO","USAS","DNN","IAG","ORLA","PAAS","BTG","JHX","AEM","NGD","CENX","AU","CDE","FCX","EQX","ARIS","SA","WPM","NG","EXP","EXK","CX","CRH","CGAU","CCJ","FNV","RGLD","NXE","NEM","CMC","BVN","KALU","SID","HBM","RS","GFI","OR","EGO","TREX","TFPM","WFG","AA","ERO","B","DRD","UUUU","SVM","SBSW","KGC","PPTA","HMY","LPX","UEC","HL/PB","USLM","HL","IE","MP","TMC","CLF","LEU"]
    },
    {
        "title": "7. Consumer Services",
        "baseSymbol": "XLY",
        "symbols": ["XLY","RCD","CHTR","SPHR","LRN","NCLH","RCL","LYV","OSW","CUK","MSGS","CCL","VIK","CAKE","DRVN","HTHT","EDU","MGM","NXST","MSGE","TNL","WH","CNK","LTH","ATAT","H","CHH","ARMK","SIRI","TKO","RRR","WYNN","VAC","MTN","CAVA","RSI","ANDG","ABNB","SHAK","MLCO","SGHC","GHC","WING","CMG","DIS","PLNT","PSKY","CZR","VSNT","TXRH","EAT","NWSA","CHDN","LION","NWS","BKNG","ATGE","FOX","BROS","FOXA","HRB","EXPE","GBTG","DKNG","FLUT"]
    },
    {
        "title": "8. Retail Trade",
        "baseSymbol": "XRT",
        "symbols": ["XRT","AAP","TPR","RUSHA","GRDN","TBBB","FIVE","RUSHB","BBWI","GAP","GME","TGT","SAH","KR","WMT","BJ","KSS","WSM","M","SIG","VSCO","BURL","ASO","DG","TSCO","DLTR","PAG","BOOT","GLBE","EYE","DKS","AEO","BBY","DDS","MELI","EBAY","URBN","CVS","FND","OLLI","SE","SFM","RH","KMX","AN","ANF","ABG","CPNG","AMZN","CPRI","MUSA","BLDR","LAD","CPRT","WRBY","ETSY","GPI","CHWY","PLBL","CVNA","W"]
    },
    {
        "title": "9. Commercial Services",
        "baseSymbol": "SPGI",
        "symbols": ["SPGI","RELY","STNE","LB","CPAY","GPN","ULS","ANDE","PAGS","NAMS","OMC","WEX","TAL","VVX","WMG","EEFT","BBU","ADT","MH","FISV","EFX","RHI","LAUR","KFY","MSCI","FOUR","TRU","CAE","BZ","PAY","WPP","FCN","FICO","GPGI","LOPE","TIC","SHOP","G","MCO","XYZ","BAH","PICS","SPGI","BFAM","CRL","MORN","MMS","MEDP","PYPL","HURN","DJT","FDS","ICLR","KLAR"]
    },
    {
        "title": "10. Consumer Staples",
        "baseSymbol": "XLP",
        "symbols": ["XLP","RHS","UAA","UA","DAR","CENT","COLM","CENTA","CROX","DECK","COKE","KTB","ZGN","HLF","TPB","PVH","VFC","LW","BF.A","BF.B","KHC","FRPT","POST","BIRK","LEVI","SAM","ONON","CAG","RL","ASH","ELF","NKE","CPB","GIS","REYN","MICC","TAP","COCO","EL","PRMB","LULU","AKO.B","STZ","SXT","FLO","SHOO","CELH","COTY","ARW","DXPE","MCK","USFD","UNFI","CHEF","SNX","QXO","GWW","CAH","WCC","SITE","COR","HSIC","REZI","AIT","CNM","BCC","DNOW","GPC","POOL"]
    },
    {
        "title": "11. Industrial Services",
        "baseSymbol": "XLI",
        "symbols": ["XLI","VAL","RIG","NE","KGS","LBRT","NESR","FIX","SDRL","STRL","FLR","WFRD","ROAD","FTI","PWR","EME","MTZ","DY","ECG","KNTK","TPC","PTEN","LGN","DKL","FLOC","AGX","PRIM","IESC","NOV","MYRG","VG","HP","VSEC","WHD","IBP","IHS","BLD","ACM","TTEK","GFL","BBUC","EXPO","CWST","STN"]
    },
    {
        "title": "12. Transportation",
        "baseSymbol": "IYT",
        "symbols": ["IYT","XTN","XPO","ZIM","TDW","STNG","LUV","SAIA","ARCB","SBLK","TNK","GXO","R","ODFL","JBLU","KNX","JBHT","MATX","CMRE","VNT","TFII","WERN","CAAP","ALK","CHRW","CPA","SKYW","AERO","PFGC","DAL","UAL","SNDR","LSTR","EXPD","LTM","GRAB","CART","HUBG","AAL","UBER","DASH","VRRM","LYFT"]
    },
    {
        "title": "13. Process Industries",
        "baseSymbol": "XLB",
        "symbols": ["XLB","SSL","SOLS","ESI","CE","EMN","DD","SW","AVNT","WDFC","LYB","KWR","DOW","AMBP","IP","WLK","BG","NGVT","HUN","OC","CALM","CF","CBT","CC","MOS","MEOH","OLN","SEB","GEL","ADM","OI","IOSP","PPC","CSW","ALB","HWKN","NEU","ALB/PA","PRM","SQM","GPK"]
    },
    {
        "title": "14. Energy",
        "baseSymbol": "XLE",
        "symbols": ["XLE","RYE","CLMT","CRGY","SM","DK","CVE","OVV","NOG","CRC","IMO","MNR","DVN","MTDR","TALO","VIST","MGY","CTRA","XPRO","EQT","BKV","APA","PARR","CHRD","BTE","RRC","MPC","YPF","PBF","CNX","MUR","GPOR","AR","DINO","EXE","EC","CNR","BTU","CVI","CRK","HCC","AMR"]
    },
    {
        "title": "15. Consumer Durables",
        "baseSymbol": "XHB",
        "symbols": ["XHB","HAS","OSK","AS","SWK","TOL","GRBK","NIO","PHM","TMHC","KBH","PATK","DHI","MHO","CCS","SN","LKQ","SKY","IMAX","NVR","LEN","THO","HOG","GT","CALY","TSLA","WHR","SGI","PII","LCID","FTDR","RIVN","XPEV","CVCO","MAT","STLA"]
    },
    {
        "title": "16. Comms, Utilities & Misc",
        "baseSymbol": "XLC",
        "symbols": ["XLC","RSPC","XLU","RYU","SKM","LBRDK","LBRDA","LBTYA","LBTYK","IRDM","TMUS","AMX","TIGO","TEO","GSAT","LUMN","SATS","ASTS","ENLT","CSAN","AXIA/PC","AXIA/P","NRG","CTRI","HE","BEPC","VST","CEPU","XIFR","TLN","TGS","TAC","PAM","CEG","ORA","AWK","TPL","WT","CEF","FSK","PSLV"]
    },
]

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
LAST_FULL_REFRESH_FILE = os.path.join(DATA_DIR, ".last_full_refresh")


def get_all_unique_tickers():
    """Extract all unique tickers across all panels."""
    tickers = set()
    for panel in PANELS:
        for sym in panel["symbols"]:
            # yfinance uses '-' instead of '/' for preferred shares
            tickers.add(sym.replace("/", "-"))
    return sorted(tickers)


def fetch_all_data(tickers, period=None, start=None, end=None):
    """Batch-download daily close prices for all tickers."""
    if start:
        print(f"Downloading {len(tickers)} tickers ({start} to {end or 'today'})...")
    else:
        print(f"Downloading {len(tickers)} tickers ({period} of daily data)...")

    batch_size = 200
    all_data = None

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        print(f"  Batch {i // batch_size + 1}: {len(batch)} tickers...")
        try:
            kwargs = dict(
                interval="1d",
                auto_adjust=True,
                threads=True,
                progress=True,
            )
            if start:
                kwargs["start"] = start
                if end:
                    kwargs["end"] = end
            else:
                kwargs["period"] = period or "5y"

            df = yf.download(batch, **kwargs)
            if df.empty:
                print(f"  Warning: empty result for batch {i // batch_size + 1}")
                continue

            # Extract just the Close prices
            if isinstance(df.columns, type(df.columns)) and hasattr(df.columns, 'levels'):
                # Multi-level columns from multi-ticker download
                if "Close" in df.columns.get_level_values(0):
                    close = df["Close"]
                else:
                    close = df
            else:
                close = df

            if all_data is None:
                all_data = close
            else:
                all_data = all_data.join(close, how="outer", rsuffix="_dup")
                # Remove any duplicate columns
                all_data = all_data[[c for c in all_data.columns if not c.endswith("_dup")]]

        except Exception as e:
            print(f"  Error in batch {i // batch_size + 1}: {e}")

    if all_data is not None:
        # Deduplicate date index (outer join across batches can create dupes)
        if all_data.index.duplicated().any():
            all_data = all_data[~all_data.index.duplicated(keep="last")]

        # Drop trailing dates where <50% of tickers have data
        # (yfinance often returns partial data for the most recent day)
        while len(all_data) > 0:
            last_row = all_data.iloc[-1]
            valid_pct = last_row.notna().sum() / len(last_row)
            if valid_pct < 0.5:
                dropped = all_data.index[-1].strftime("%Y-%m-%d")
                all_data = all_data.iloc[:-1]
                print(f"  Dropped sparse trailing date {dropped} ({valid_pct:.0%} coverage)")
            else:
                break

    return all_data


def build_panel_json(panel, close_data, panel_index):
    """Build the columnar JSON for a single panel."""
    # Get the dates index
    dates = [d.strftime("%Y-%m-%d") for d in close_data.index]

    prices = {}
    included_symbols = []

    for sym in panel["symbols"]:
        # Try the ticker as-is first, then with '/' replaced by '-'
        yf_sym = sym.replace("/", "-")
        col = None
        if yf_sym in close_data.columns:
            col = yf_sym
        elif sym in close_data.columns:
            col = sym

        if col is not None:
            series = close_data[col]
            # Convert to list, replacing NaN with None
            price_list = []
            for val in series:
                if val is not None and not (isinstance(val, float) and val != val):
                    price_list.append(round(float(val), 4))
                else:
                    price_list.append(None)
            prices[sym] = price_list
            included_symbols.append(sym)
        else:
            print(f"  Panel {panel_index + 1}: ticker '{sym}' not found in data, skipping")

    return {
        "title": panel["title"],
        "baseSymbol": panel["baseSymbol"],
        "symbols": included_symbols,
        "dates": dates,
        "prices": prices,
    }


def save_last_full_refresh():
    """Record today's date as the last full refresh."""
    with open(LAST_FULL_REFRESH_FILE, "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d"))


def get_last_full_refresh():
    """Return the date of the last full refresh, or None."""
    if not os.path.exists(LAST_FULL_REFRESH_FILE):
        return None
    try:
        with open(LAST_FULL_REFRESH_FILE) as f:
            return datetime.strptime(f.read().strip(), "%Y-%m-%d")
    except (ValueError, OSError):
        return None


def check_full_refresh_reminder():
    """Print a reminder if it's been >30 days since last full refresh."""
    last = get_last_full_refresh()
    if last is None:
        print("Note: No full refresh date recorded. Consider running with --mode full.")
        return
    days_ago = (datetime.now() - last).days
    if days_ago > 30:
        print(f"Note: Last full refresh was {days_ago} days ago. "
              f"Run with --mode full periodically to pick up split adjustments.")


def get_last_date():
    """Read panel_01.json and return the last date string, or None."""
    path = os.path.join(DATA_DIR, "panel_01.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        if data.get("dates"):
            return data["dates"][-1]
    except (json.JSONDecodeError, OSError):
        pass
    return None


def incremental_update():
    """Append new trading days to existing panel JSON files."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Incremental update started")

    last_date_str = get_last_date()
    if not last_date_str:
        print("ERROR: No existing data found. Run with --mode full first.")
        sys.exit(1)

    last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
    start_date = last_date + timedelta(days=1)
    today = datetime.now()

    print(f"Last date in data: {last_date_str}")
    print(f"Fetching from: {start_date.strftime('%Y-%m-%d')}")

    if start_date.date() > today.date():
        print("Already up to date. No new data to fetch.")
        return

    check_full_refresh_reminder()

    # Collect all unique tickers
    all_tickers = get_all_unique_tickers()
    print(f"Total unique tickers: {len(all_tickers)}")

    # Fetch only the new date range
    close_data = fetch_all_data(
        all_tickers,
        start=start_date.strftime("%Y-%m-%d"),
    )

    if close_data is None or close_data.empty:
        print("No new trading data available (weekend/holiday?).")
        return

    new_dates = [d.strftime("%Y-%m-%d") for d in close_data.index]
    print(f"\nFetched {len(new_dates)} new trading day(s): {new_dates[0]} to {new_dates[-1]}")

    # Append to each panel JSON
    for i, panel in enumerate(PANELS):
        filename = f"panel_{i + 1:02d}.json"
        filepath = os.path.join(DATA_DIR, filename)

        # Load existing data
        try:
            with open(filepath) as f:
                existing = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            print(f"  WARNING: {filename} missing or corrupt, skipping. Run --mode full to fix.")
            continue

        # Filter out any dates already in the existing data (prevent dupes)
        existing_date_set = set(existing["dates"])
        dates_to_add = [d for d in new_dates if d not in existing_date_set]
        if not dates_to_add:
            print(f"  {filename}: all dates already present, skipping")
            continue
        date_mask = [d in set(dates_to_add) for d in new_dates]

        # Append new dates
        existing["dates"].extend(dates_to_add)

        # Append new prices for each symbol already in the panel
        symbols_in_new_data = set()
        for sym in existing["symbols"]:
            yf_sym = sym.replace("/", "-")
            col = None
            if yf_sym in close_data.columns:
                col = yf_sym
            elif sym in close_data.columns:
                col = sym

            if col is not None:
                symbols_in_new_data.add(sym)
                series = close_data[col]
                for j, val in enumerate(series):
                    if not date_mask[j]:
                        continue
                    if val is not None and not (isinstance(val, float) and val != val):
                        existing["prices"][sym].append(round(float(val), 4))
                    else:
                        existing["prices"][sym].append(None)
            else:
                # Symbol not in new download — fill with nulls
                for _ in dates_to_add:
                    existing["prices"][sym].append(None)

        # Check for symbols in panel definition but not in existing data
        existing_syms = set(existing["symbols"])
        panel_syms = set(panel["symbols"])
        new_syms = panel_syms - existing_syms
        if new_syms:
            print(f"  WARNING: {filename} missing symbols {new_syms}. "
                  f"Run --mode full to add them with full history.")

        # Write updated file
        with open(filepath, "w") as f:
            json.dump(existing, f)

        size_kb = os.path.getsize(filepath) / 1024
        print(f"  Updated {filename}: +{len(new_dates)} dates, {size_kb:.0f} KB")

    print(f"\nDone! {len(PANELS)} panel files updated with {len(new_dates)} new trading day(s).")


def full_refresh():
    """Full 5-year download — original behavior."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Full refresh started")

    os.makedirs(DATA_DIR, exist_ok=True)

    # Collect all unique tickers
    all_tickers = get_all_unique_tickers()
    print(f"Total unique tickers: {len(all_tickers)}")

    # Fetch all data at once
    close_data = fetch_all_data(all_tickers, period="5y")

    if close_data is None or close_data.empty:
        print("ERROR: No data was downloaded. Check your internet connection.")
        sys.exit(1)

    print(f"\nDownloaded data for {len(close_data.columns)} tickers, {len(close_data)} trading days")
    print(f"Date range: {close_data.index[0].strftime('%Y-%m-%d')} to {close_data.index[-1].strftime('%Y-%m-%d')}")

    # Build and write JSON for each panel
    for i, panel in enumerate(PANELS):
        panel_json = build_panel_json(panel, close_data, i)
        filename = f"panel_{i + 1:02d}.json"
        filepath = os.path.join(DATA_DIR, filename)

        with open(filepath, "w") as f:
            json.dump(panel_json, f)

        size_kb = os.path.getsize(filepath) / 1024
        print(f"  Wrote {filename}: {len(panel_json['symbols'])} symbols, {size_kb:.0f} KB")

    # Build stock names mapping
    print("\nFetching company names...")
    names = {}
    batch_size = 100
    for i in range(0, len(all_tickers), batch_size):
        batch = all_tickers[i:i + batch_size]
        try:
            tickers_obj = yf.Tickers(" ".join(batch))
            for sym in batch:
                try:
                    info = tickers_obj.tickers[sym].info
                    name = info.get("shortName") or info.get("longName") or ""
                    if name:
                        # Restore original symbol format (with '/')
                        orig_sym = sym.replace("-", "/") if "-" in sym and sym.replace("-", "/") in {
                            s for p in PANELS for s in p["symbols"]
                        } else sym
                        names[orig_sym] = name
                except Exception:
                    pass
        except Exception as e:
            print(f"  Error in names batch {i // batch_size + 1}: {e}")
        print(f"  Names batch {i // batch_size + 1}: {len(batch)} tickers")

    names_path = os.path.join(DATA_DIR, "stock_names.json")
    with open(names_path, "w") as f:
        json.dump(names, f, indent=0)
    print(f"  Wrote stock_names.json: {len(names)} names, {os.path.getsize(names_path) / 1024:.0f} KB")

    save_last_full_refresh()
    print(f"\nDone! {len(PANELS)} JSON files + stock_names.json written to {DATA_DIR}/")


def main():
    parser = argparse.ArgumentParser(description="Fetch sector relative strength data via yfinance.")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="full = re-download 5y history (default); incremental = append new days only",
    )
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    if args.mode == "incremental":
        incremental_update()
    else:
        full_refresh()


if __name__ == "__main__":
    main()
