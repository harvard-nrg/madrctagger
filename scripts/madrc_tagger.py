#!/usr/bin/env python

import os
import re
import sys
import json
import yaml
import yaxil
import string
import logging
import requests
import collections
import argparse as ap
from io import StringIO
from yaml.representer import Representer
from yaxil.exceptions import NoExperimentsError

logger = logging.getLogger(os.path.basename(__file__))
logging.basicConfig(level=logging.INFO)

yaml.add_representer(collections.defaultdict, Representer.represent_dict)

def main():
    parser = ap.ArgumentParser()
    parser.add_argument('-a', '--alias', default='cbscentral',
        help='XNAT alias')
    parser.add_argument('-c', '--cache', action='store_true',
        help='Speed up development by caching yaxil.scans output')
    parser.add_argument('-o', '--output-file',
        help='Output summary of updates')
    parser.add_argument('--protocol', choices=['v1', 'v2'], default='v1',
        help='Scan protocol')
    parser.add_argument('--confirm', action='store_true',
        help='Prompt user to confirm every update')
    parser.add_argument('--overwrite', action='store_true',
        help='Overwrite note instead of appending')
    parser.add_argument('--dry-run', action='store_true',
        help='Do everything except change anything in XNAT')
    parser.add_argument('session')
    args = parser.parse_args()
    
    try:
        scans = get_scan_listing(args.session, args.alias, args.cache)
    except NoExperimentsError as e:
        logger.critical(f'could not find {args.session} in {args.alias}')
        sys.exit(1)

    updates = {
        'csx6':   csx6(scans, args.protocol),
        'wave':   wave(scans, args.protocol),
        'adni':   adni(scans, args.protocol),
        'diffb0': diffb0(scans, args.protocol)
    }

    if not args.dry_run:
        upsert(
            args.alias,
            scans,
            updates,
            overwrite=args.overwrite,
            confirm=args.confirm
        )

    if args.output_file:
        logger.info(f'saving {args.output_file}')
        with open(args.output_file, 'w') as fo:
            content = yaml.dump(updates, sort_keys=False)
            fo.write(content)

def upsert(alias, scans, updates, overwrite=False, confirm=False):
    auth = yaxil.auth(alias)
    updates = list(squeeze(updates))
    t1w = 0
    for scan in scans:
        sid = scan['id']
        note = scan['note']
        update = [x for x in updates if x['scan'] == sid]
        if not update:
            continue
        if len(update) > 1:
            raise UpsertError(f'found too many updates for scan {sid}')
        update = update.pop()
        note = update['note'].strip()
        tag = update['tag'].strip()
        modality = update['modality'].strip()
        if modality.lower() == 't1w':
            t1w += 1
        if tag not in note:
            upsert = tag
            if note and not overwrite:
                upsert = f'{tag} {note}'
            if modality.lower() == 't1w':
              upsert = f'{upsert} #T1w_{t1w}'
            logger.info(f'setting note for scan {sid} to "{upsert}"')
            if confirm:
                input('press enter to continue')
            setnote(auth, scan, text=upsert)

class UpsertError(Exception):
    pass

def squeeze(updates):
    for _,voxels in iter(updates.items()):
        for _,items in iter(voxels.items()):
            for item in items:
                yield item

def setnote(auth, scan, text=None):
    if not text:
        text = ' '
    project = scan['session_project']
    subject = scan['subject_label'] 
    session = scan['session_label']
    scan_id = scan['id']
    baseurl = auth.url.rstrip('/')
    url = f'{baseurl}/data/projects/{project}/subjects/{subject}/experiments/{session}/scans/{scan_id}'
    params = {
        'xnat:mrscandata/note': text
    }
    logger.info(f'setting note for {session} scan {scan_id} to {text}')
    logger.info(f'PUT {url} params {params}')
    r = requests.put(url, params=params, auth=(auth.username, auth.password))
    if r.status_code != requests.codes.OK:
        raise SetNoteError(f'response not ok for {url}')

class SetNoteError(Exception):
    pass

def adni(scans, protocol):
    match protocol:
      case 'v1':
        adnifilter = adnifilter_v1
      case 'v2':
        adnifilter = adnifilter_v2
    scans = filter(adnifilter, scans)
    groups = collections.defaultdict(list)
    for scan in scans:
        sid = scan['id']
        session = scan['session_label']
        series = scan['series_description'].strip()
        note = scan['note'].strip()
        vox = scan['vox_x']
        tag = f'ANAT_{vox}_ADNI'
        groups[vox].append({
            'project': scan['session_project'],
            'subject': scan['subject_label'],
            'session': session, 
            'scan': sid,
            'modality': 't1w',
            'series_description': series,
            'note': note,
            'tag': tag
        })
    return groups

def adnifilter_v1(x):
    return (
        x['series_description'] == 'Accelerated Sagittal MPRAGE (MSV21)' and
        x['quality'] == 'usable'
    )

def adnifilter_v2(x):
    return (
        x['series_description'] == 'Accelerated Sagittal MPRAGE' and
        x['quality'] == 'usable'
    )

def csx6(scans, protocol):
    match protocol:
      case 'v1':
        csx6filter = csx6filter_v1
      case 'v2':
        csx6filter = csx6filter_v2
    scans = filter(csx6filter, scans)
    groups = collections.defaultdict(list)
    for scan in scans:
        sid = scan['id']
        session = scan['session_label']
        series = scan['series_description'].strip()
        note = scan['note'].strip()
        match = re.match('.*_(\d+)mmCor_.*', series)
        if match:
            vox = float(match.group(1))
            suffix = string.ascii_lowercase[len(groups[vox])]
            tag = f'ANAT_{vox}_CSx6_{suffix}'
            groups[vox].append({
                'project': scan['session_project'],
                'subject': scan['subject_label'],
                'session': session, 
                'scan': sid,
                'modality': 't1w',
                'series_description': series,
                'note': note,
                'tag': tag
            })
    return groups

def csx6filter_v1(x):
    expr = re.compile('^WIP925B_\d+\.\d+mmCor_\d+_\d+_CSx6$')
    image_type = x.get('image_type', '').encode('utf-8').decode('unicode_escape')
    return (
        expr.match(x['series_description']) and 
        image_type == 'ORIGINAL\\PRIMARY\\M\\ND\\NORM' and
        x['quality'] == 'usable'
    )

def csx6filter_v2(x):
    expr = re.compile('^WIP19_1mmCor_\d+_\d+_CSx6$')
    image_type = x.get('image_type', '').encode('utf-8').decode('unicode_escape')
    return (
        expr.match(x['series_description']) and 
        image_type == 'ORIGINAL\\PRIMARY\\M\\NONE' and
        x['quality'] == 'usable'
    )

def wave(scans, protocol):
    rr_num,rr_tag = 0,0
    scans = filter(wavefilter, scans)
    groups = collections.defaultdict(list)
    for scan in scans:
        sid = scan['id']
        session = scan['session_label']
        series = scan['series_description'].strip()
        note = scan['note'].strip()
        match = re.match('.*_(\d+)mm(_RR)?', series)
        if match:
            vox = float(match.group(1))
            is_rr = match.group(2)
            suffix = string.ascii_lowercase[len(groups[vox])]
            tag = f'ANAT_{vox:.1f}_WAVE'
            # 🤷 special handling of RR (retro-recon) scans
            if is_rr:
                rr_num += 1
                mod = rr_num % 2
                rr_tag += mod
                if mod == 0:
                    rr_res = '0.0'
                else:
                    rr_res = '0.1'
                tag += f'_RR{rr_tag}_{rr_res}'
            tag += f'_{suffix}'
            groups[vox].append({
                'project': scan['session_project'],
                'subject': scan['subject_label'],
                'session': session,
                'scan': sid,
                'modality': 't1w',
                'series_description': series,
                'note': note,
                'tag': tag
            })
    return groups

def wavefilter(x):
    expr = re.compile('^WIP1084C_r3x3_1mm(_RR)?$')
    image_type = x.get('image_type', '').encode('utf-8').decode('unicode_escape')
    return (
        expr.match(x['series_description']) and
        image_type == 'ORIGINAL\\PRIMARY\\M\\ND\\NORM' and
        x['quality'] == 'usable'
    )

def diffb0(scans, protocol):
    scans = filter(diffb0filter, scans)
    groups = collections.defaultdict(list)
    count = 1
    for scan in scans:
        sid = scan['id']
        session = scan['session_label']
        series = scan['series_description'].strip()
        note = scan['note'].strip()
        match = re.match('.*_(\d+)mm_.*', series)
        if match:
            if count > 2:
                raise DiffB0Error('found too many diff b0 scans')
            vox = float(match.group(1))
            suffix = 'Set12' if count == 1 else 'Set34'
            tag = f'DIFF_{vox:.1f}_4B0_{suffix}'
            groups[vox].append({
                'project': scan['session_project'],
                'subject': scan['subject_label'],
                'session': session,
                'scan': sid,
                'modality': 'b0',
                'series_description': series,
                'note': note,
                'tag': tag
            })
            count += 1
    return groups

def diffb0filter(x):
    return (
        x['series_description'] == 'CMRR_DiffPA_2mm_4b0' and
        x['quality'] == 'usable'
    )

def get_scan_listing(session, alias='cbscentral', cache=False):
    '''
    Return scan listing as a list of dictionaries. 
    
    This function attempts to read the scan listing from a 
    cached JSON file. However, if a cached file doesn't exist, 
    one will be created by saving the output from yaxil.scans.
    '''
    cachefile = f'{session}.json'
    scans = None
    if not os.path.exists(cachefile):
        logger.info(f'cache miss {cachefile}')
        auth = yaxil.auth(alias)
        scans = list(yaxil.scans(auth, label=session))
        if cache:
            with open(cachefile, 'w') as fo:
                fo.write(json.dumps(scans, indent=2))
    else:
        logger.info(f'cache hit {cachefile}')
        with open(cachefile) as fo:
            scans = json.loads(fo.read())
    return scans

class DiffB0Error(Exception):
    pass

if __name__ == '__main__':
    main()
