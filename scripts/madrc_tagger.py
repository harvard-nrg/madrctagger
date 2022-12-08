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
    parser.add_argument('--confirm', action='store_true',
        help='Prompt user to confirm every update')
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
        'csx6': csx6(scans),
        'wave': wave(scans),
        'adni': adni(scans),
        'diffb0': diffb0(scans)
    }

    if not args.dry_run:
        upsert(args.alias, scans, updates, confirm=args.confirm)

    if args.output_file:
        logger.info(f'saving {args.output_file}')
        with open(args.output_file, 'w') as fo:
            content = yaml.dump(updates, sort_keys=False)
            fo.write(content)

def upsert(alias, scans, updates, confirm=False):
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
        if tag not in note:
            upsert = tag
            if note:
                upsert = f'{tag} {note}'
            if modality.lower() == 't1w':
              t1w += 1
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

def adni(scans):
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

def adnifilter(x):
    return (
        x['series_description'] == 'Accelerated Sagittal MPRAGE (MSV21)' and
        x['quality'] == 'usable'
    )

def csx6(scans):
    scans = filter(csx6filter, scans)
    groups = collections.defaultdict(list)
    for scan in scans:
        sid = scan['id']
        session = scan['session_label']
        series = scan['series_description'].strip()
        note = scan['note'].strip()
        match = re.match('.*_(\d+\.\d+)mmCor_.*', series)
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

def csx6filter(x):
    expr = re.compile('^WIP925B_\d+\.\d+mmCor_\d+_\d+_CSx6$')
    image_type = x.get('image_type', '').encode('utf-8').decode('unicode_escape')
    return (
        expr.match(x['series_description']) and 
        image_type == 'ORIGINAL\\PRIMARY\\M\\ND\\NORM' and
        x['quality'] == 'usable'
    )

def wave(scans):
    scans = filter(wavefilter, scans)
    groups = collections.defaultdict(list)
    for scan in scans:
        sid = scan['id']
        session = scan['session_label']
        series = scan['series_description'].strip()
        note = scan['note'].strip()
        match = re.match('.*_(\d+)mm', series)
        if match:
            vox = float(match.group(1))
            suffix = string.ascii_lowercase[len(groups[vox])]
            tag = f'ANAT_{vox:.1f}_WAVE_{suffix}'
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

def diffb0(scans):
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
