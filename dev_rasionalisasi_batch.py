#!/usr/bin/env python
# coding: utf-8

import sys
from os.path import abspath,join
sys.path.append(abspath(join('..', 'module')))
from openTable import *
from AccesDB import *
from data_dictionary import *

import numpy as np
import pandas as pd
from statistics import mean
from datetime import date

import math
import random

tps = ['nilai_penalaran', 'nilai_kuantitatif', 'nilai_umum', 'nilai_baca_tulis']
tpa_saintek = ['nilai_mat_saintek', 'nilai_fisika', 'nilai_kimia', 'nilai_biologi']
tpa_soshum = ['nilai_mat_soshum', 'nilai_geografi', 'nilai_sejarah', 'nilai_sosiologi', 'nilai_ekonomi']
saintek = tps + tpa_saintek
soshum = tps + tpa_soshum
semua_nilai = tps + tpa_saintek + tpa_soshum
rasionalisasi_pilihan = ['userId','facultydepartmentid']

def get_hasil_rangking(num, minimum, minimum_atas):
    if num >= minimum_atas:
        return "Kemungkinan Lolos Besar"
    elif num < minimum_atas and num >= minimum:
        return "Kemungkinan Lolos Sedang"
    else:
        return "Kemungkinan Lolos Kecil"
    
def get_tabel_peserta(koneksi, server, statement, kolom_jenis, ncol=semua_nilai):
    pilihan_pendaftar, status = open_table_ds(koneksi, ['*'],'dev_rasionalisasi_pilihan',statement)
    tabel_pilihan = pd.DataFrame(pilihan_pendaftar)
    tabel_pilihan = tabel_pilihan.rename(columns={0:'userId', 1:'jurusanId', 2:'facultydepartmentid', 3:'rangking', 
                                                  4:'tanggal', 5:'indexs_pilihan', 6:'recommendation'})

    nilai, status = open_table_ds(koneksi,['*'],'dev_rasionalisasi_nilai')
    tabel_nilai = pd.DataFrame(nilai)
    tabel_nilai = tabel_nilai.drop(columns=15)
    tabel_nilai = tabel_nilai.rename(columns={0:'userId', 1:'jenis', 2:ncol[0], 3:ncol[1], 4:ncol[2], 5:ncol[3], 6:ncol[4], 
                                              7:ncol[5], 8:ncol[6], 9:ncol[7], 10:ncol[8], 11:ncol[9], 12:ncol[10], 
                                              13:ncol[11], 14:ncol[12]})
    tabel_nilai = tabel_nilai[['userId']+[x for x in kolom_jenis]]
    mean_nilai = tabel_nilai[[x for x in kolom_jenis]].mean(axis = 1, skipna = True)
    tabel_nilai['rerata'] = mean_nilai
    return tabel_pilihan[['userId','facultydepartmentid','indexs_pilihan']].merge(tabel_nilai)

def get_rerata_jurusan(tabel_pendaftar, kolom_jenis):
    rerata_jurusan = {}
    rerata_jurusan['tahun_ini_nilai_rerata'] = mean(tabel_pendaftar['rerata'])
    rerata_jurusan['tahun_ini_pendaftar'] = int(len(tabel_pendaftar))
    for mata_uji in kolom_jenis:
        rerata_jurusan['rerata_'+mata_uji] = mean(list(tabel_pendaftar[mata_uji]))    
    rerata_jurusan['tanggal'] = date.today().strftime('%Y-%m-%d')
    return rerata_jurusan

def get_batch_jurusan(jurusan, ncol=semua_nilai, saintek=saintek, soshum=soshum, tps=tps):
    ds_server, ds_koneksi = Connection_2()
    statement = ' WHERE facultydepartmentid = ' + str(jurusan)

    col = ['indexs', 'facultydepartmentid', 'jenis', 'tahun_lalu_rerata', 'tahun_lalu_sbaku', 
           'tahun_ini_kuota', 'tahun_lalu_peminat']
    tables = 'dev_rasionalisasi_universitas'

    kampus, status = open_table_ds(ds_koneksi, col, tables, statement)
    ind = kampus[0][0]
    jenis = kampus[0][2].lower()
    if jenis == 'saintek':
        kolom_jenis = saintek
    elif jenis == 'soshum':
        kolom_jenis = soshum
    elif jenis == 'tps':
        kolom_jenis = tps
    rerata_lalu = kampus[0][3]
    s_baku_lalu = kampus[0][4]
    kuota = kampus[0][5]
    peminat_lalu = kampus[0][6]
    tabel_pendaftar = get_tabel_peserta(ds_koneksi, ds_server, statement, kolom_jenis)
    rerata_jurusan = get_rerata_jurusan(tabel_pendaftar, kolom_jenis)
    
    statementnew = ' WHERE indexs = ' + str(ind)
    update_db(ds_koneksi, tables, rerata_jurusan, statementnew)

    kuota_atas = int(kuota*0.2)
    if peminat_lalu > kuota:
        generate = np.random.normal(rerata_lalu, s_baku_lalu, kuota)
        tabel_generate = pd.DataFrame(generate, columns = ['rerata'])
        tabel_generate['indexs_pilihan'] = tabel_pendaftar['indexs_pilihan'].values[0]
        tabel_perangkingan = tabel_pendaftar.append(tabel_generate)
        nilai_minimum = tabel_perangkingan.sort_values('rerata',ascending = False).head(int(kuota)).tail(1)['rerata'].values[0]
        nilai_minimum_atas = tabel_perangkingan.sort_values('rerata',ascending = False).head(int(kuota_atas)).tail(1)['rerata'].values[0]
    else:
        tabel_perangkingan = tabel_pendaftar
        nilai_minimum = 500
        nilai_minimum_atas = 650
    tabel_perangkingan = tabel_perangkingan.reset_index(drop=True)

    hasil_perangkingan = []
    for index, row in tabel_perangkingan.iterrows():
        hasil_perangkingan.append(get_hasil_rangking(row['rerata'], nilai_minimum, nilai_minimum_atas))
    tabel_perangkingan['recommendation'] = hasil_perangkingan
    tabel_perangkingan = tabel_perangkingan[np.isfinite(tabel_perangkingan['userId'])]
    tabel_perangkingan['userId'] = tabel_perangkingan['userId'].astype('int')
    tabel_perangkingan['indexs_pilihan'] = tabel_perangkingan['indexs_pilihan'].astype('int')
    tabel_perangkingan = tabel_perangkingan.sort_values('rerata',ascending = False).reset_index(drop=True)

    for index, row in tabel_perangkingan.iterrows():
        result = {}
        indeks_pilihan = row['indexs_pilihan']
        statement_update = ' WHERE indexs = ' + str(indeks_pilihan)
        result['ranking'] = index + 1
        result['tanggal'] = date.today().strftime('%Y-%m-%d')
        result['recommendation'] = row['recommendation']
        update_db(ds_koneksi,'dev_rasionalisasi_pilihan', result, statement_update)

    ds_koneksi.commit()
    ds_koneksi.close()
    ds_server.stop()

    return tabel_perangkingan

def dev_rasionalisasi_batch():
    ds_server, ds_koneksi = Connection_2()
    df, status = open_table_ds(ds_koneksi, ['*'], 'dev_rasionalisasi_pilihan')
    ds_koneksi.commit()
    ds_koneksi.close()
    ds_server.stop()
    df = pd.DataFrame(df)
    for i in set(df[2].to_list()):
        get_batch_jurusan(i)

dev_rasionalisasi_batch()