#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OnlyWaves — Puente de internals IBKR → Worker
Lee TICK-NYSE / AD-NYSE (+UVOL/DVOL si el feed los entrega) cada 30 s
dentro de las ventanas operativas y los empuja a POST /internals del Worker.

Diseñado para GitHub Actions (cron cada 15 min dentro de las ventanas):
cada invocación corre hasta el fin de la ventana o 14 min, lo que llegue antes.
Ventanas (hora AR = UTC-3 fija): 11:15–12:45 y 16:50–17:10.

Secrets requeridos (GitHub → Settings → Secrets and variables → Actions):
  IBKR_CONSUMER_KEY, IBKR_ACCESS_TOKEN, IBKR_ACCESS_TOKEN_SECRET,
  IBKR_SIGNATURE_KEY_PEM, IBKR_ENCRYPTION_KEY_PEM  (contenido completo de los .pem),
  IBKR_DH_PRIME  (contenido de dh_prime.txt),
  INTERNALS_TOKEN  (el mismo que se carga como secret del Worker)
"""
import os, sys, time, json, datetime as dt, tempfile, urllib.request

WORKER_URL = "https://onlywaves-api.onlysongs-app.workers.dev/internals"
SYMBOLS = ["TICK-NYSE", "AD-NYSE"]          # núcleo
SYMBOLS_OPT = ["UVOL-NYSE", "DVOL-NYSE"]     # se prueban; si el feed no los da, se ignoran
CADENCE_S = 30
MAX_RUN_S = 14 * 60

def now_utc(): return dt.datetime.now(dt.timezone.utc)

def in_window(t=None):
    """Ventanas en UTC (AR+3): 14:15–15:45 y 19:50–20:10, lun-vie."""
    t = t or now_utc()
    if t.weekday() > 4: return None
    hm = t.hour * 60 + t.minute
    for a, b in ((14*60+15, 15*60+45), (19*60+50, 20*60+10)):
        if a <= hm < b:
            return t.replace(hour=b//60, minute=b%60, second=0, microsecond=0)
    return None

def write_pem(env_name, fname):
    v = os.environ.get(env_name, "")
    if not v: sys.exit(f"Falta secret {env_name}")
    p = os.path.join(tempfile.gettempdir(), fname)
    open(p, "w").write(v)
    return p

def make_client():
    os.environ['IBIND_USE_OAUTH'] = 'True'
    os.environ['IBIND_OAUTH1A_CONSUMER_KEY'] = os.environ['IBKR_CONSUMER_KEY']
    os.environ['IBIND_OAUTH1A_ACCESS_TOKEN'] = os.environ['IBKR_ACCESS_TOKEN']
    os.environ['IBIND_OAUTH1A_ACCESS_TOKEN_SECRET'] = os.environ['IBKR_ACCESS_TOKEN_SECRET']
    os.environ['IBIND_OAUTH1A_SIGNATURE_KEY_FP'] = write_pem('IBKR_SIGNATURE_KEY_PEM', 'sig.pem')
    os.environ['IBIND_OAUTH1A_ENCRYPTION_KEY_FP'] = write_pem('IBKR_ENCRYPTION_KEY_PEM', 'enc.pem')
    os.environ['IBIND_OAUTH1A_DH_PRIME'] = os.environ['IBKR_DH_PRIME'].strip()
    from ibind import IbkrClient
    return IbkrClient(use_oauth=True)

def resolve_conids(c):
    """Resuelve conids por símbolo. Cachea en archivo del runner (efímero, se rehace cada run: barato)."""
    conids, missing = {}, []
    for sym in SYMBOLS + SYMBOLS_OPT:
        try:
            r = c.search_contract_by_symbol(sym).data or []
            hit = next((x for x in r if 'NYSE' in (x.get('description') or '')), r[0] if r else None)
            if hit and hit.get('conid'):
                conids[sym] = int(hit['conid'])
            else:
                missing.append(sym)
        except Exception as e:
            missing.append(sym)
    core_missing = [s for s in SYMBOLS if s not in conids]
    if core_missing:
        sys.exit(f"No se resolvieron conids núcleo: {core_missing}")
    if missing:
        print(f"[info] sin conid (se omiten): {missing}")
    return conids

def snapshot(c, conids):
    """Último precio (campo 31) por conid."""
    ids = list(conids.values())
    r = c.live_marketdata_snapshot(ids, fields=['31']).data or []
    px = {}
    by_conid = {int(x.get('conid', -1)): x for x in r if isinstance(x, dict)}
    for sym, cid in conids.items():
        raw = (by_conid.get(cid) or {}).get('31')
        try: px[sym] = float(str(raw).replace(',', ''))
        except (TypeError, ValueError): px[sym] = None
    return px

def push(payload):
    req = urllib.request.Request(
        WORKER_URL, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {os.environ['INTERNALS_TOKEN']}"},
        method="POST")
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.status

def main():
    end = in_window()
    if not end:
        print("Fuera de ventana — salgo."); return
    c = make_client()
    conids = resolve_conids(c)
    print("conids:", conids)
    t_stop = min(end, now_utc() + dt.timedelta(seconds=MAX_RUN_S))
    ok = fail = 0
    while now_utc() < t_stop:
        try:
            px = snapshot(c, conids)
            payload = {"ts": int(time.time()),
                       "tick": px.get("TICK-NYSE"), "add": px.get("AD-NYSE"),
                       "uvol": px.get("UVOL-NYSE"), "dvol": px.get("DVOL-NYSE")}
            if payload["uvol"] is not None and payload["dvol"] is not None:
                payload["vold"] = payload["uvol"] - payload["dvol"]
            st = push(payload); ok += 1
            print(f"{now_utc():%H:%M:%S} push {st} tick={payload['tick']} add={payload['add']} vold={payload.get('vold')}")
        except Exception as e:
            fail += 1
            print(f"{now_utc():%H:%M:%S} ERROR ({fail}): {e}")
            if fail >= 5: sys.exit("Demasiados errores seguidos, corto.")
        time.sleep(CADENCE_S)
    print(f"Fin de tramo. pushes OK={ok}, errores={fail}")

if __name__ == "__main__":
    main()
