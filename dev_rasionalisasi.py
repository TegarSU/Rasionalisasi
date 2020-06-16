#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

import sys
from os.path import abspath,join
sys.path.append(abspath(join('..', 'module')))
from openTable import *
from AccesDB import *

from statistics import mean
from datetime import date
import math

tps = ['nilai_penalaran', 'nilai_kuantitatif', 'nilai_umum', 'nilai_baca_tulis']
tpa_saintek = ['nilai_mat_saintek', 'nilai_fisika', 'nilai_kimia', 'nilai_biologi']
tpa_soshum = ['nilai_mat_soshum', 'nilai_geografi', 'nilai_sejarah', 'nilai_sosiologi', 'nilai_ekonomi']
saintek = tps + tpa_saintek
soshum = tps + tpa_soshum
semua_nilai = tps + tpa_saintek + tpa_soshum
rasionalisasi_pilihan = ['userId','facultydepartmentid']

def dev_input_nilai_utbk(event, col=semua_nilai, columns_pilihan=rasionalisasi_pilihan):
    columns_nilai = []
    values_nilai = []
    nilai_utbk = []
    status_input = []
    for key,value in event.items():
        if key == 'pilihan_facultydepartmentid':
            pass
        else:
            columns_nilai.append(str(key))
            values_nilai.append(value)
            if key in col:
                nilai_utbk.append(value)
          
    columns_nilai.append('nilai_rerata')
    values_nilai.append(mean(nilai_utbk))
    # input nilai rasionalisasi
    ds_server,ds_koneksi = Connection_2()
    rasionalisasi_nilai_table = 'dev_rasionalisasi_nilai'
    status_input.append(to_db(ds_koneksi,rasionalisasi_nilai_table,columns_nilai,values_nilai))
    # input pilihan rasionalisasi
    rasionalisasi_pilihan_table = 'dev_rasionalisasi_pilihan'
    for i in event['pilihan_facultydepartmentid']:
        value_pilihan = [event['userId'], i]
        status_input.append(to_db(ds_koneksi,rasionalisasi_pilihan_table,columns_pilihan,value_pilihan))
        
    ds_koneksi.commit()
    ds_koneksi.close()
    ds_server.stop()
    return status_input

def dev_tambah_pilihan(event, columns=rasionalisasi_pilihan):
    ds_server,ds_koneksi = Connection_2()
    status_update = []
    table = 'dev_rasionalisasi_pilihan'
    for i in event['pilihan_facultydepartmentid']:
        values = [event['userId'], i]
        status_update.append(to_db(ds_koneksi,table,columns,values))
        
    ds_koneksi.commit()
    ds_koneksi.close()
    ds_server.stop()
    return status_update

def dev_edit_nilai_utbk(event, col=semua_nilai):
    ds_server,ds_koneksi = Connection_2()
    status_edit = []
    userId = event['userId']
    tables = 'dev_rasionalisasi_nilai'
    statement = " WHERE userId = " + str(userId)
    status_edit.append(update_db(ds_koneksi, tables, event, statement))
    ds_koneksi.commit()
    
    nilai,status = open_table_ds(ds_koneksi,col, tables, statement)
    nilai = [x for x in list(nilai[0]) if x is not None]
    nilai_rerata = {'nilai_rerata':mean(nilai)}
    status_edit.append(update_db(ds_koneksi, tables, nilai_rerata, statement))
    
    ds_koneksi.commit()
    ds_koneksi.close()
    ds_server.stop()
    
    return status_edit

def dev_hasil_rasionalisasi(event, col_nilai=semua_nilai):
    ds_server,ds_koneksi = Connection_2()

    userId = event['userId']
    jurusan = event['facultydepartmentid']
    col_kampus = ['rerata_'+x for x in col_nilai] + ['tahun_ini_pendaftar','tahun_ini_kuota','tahun_lalu_peminat']

    statement = ' WHERE userId = ' + str(userId)
    statement_tambah = ' AND facultydepartmentid = ' + str(jurusan)
    kp = ' WHERE facultydepartmentid = ' + str(jurusan)

    data_nilai, status = open_table_ds(ds_koneksi,col_nilai,'dev_rasionalisasi_nilai',statement)
    data_pilihan, status = open_table_ds(ds_koneksi,['ranking','recommendation'],
                                         'dev_rasionalisasi_pilihan',statement+statement_tambah)
    data_kampus, status = open_table_ds(ds_koneksi,col_kampus,'dev_rasionalisasi_universitas', kp)
    
    ds_koneksi.close()
    ds_server.stop()

    nilai_user = {}
    for i in range(0,len(data_nilai[0])):
        if data_nilai[0][i] is not None:
            nilai_user[str(col_nilai[i])] = data_nilai[0][i]

    nilai_jurusan = {}
    for i in range(0,len(data_kampus[0])-3):
        if data_kampus[0][i] is not None:
            nilai_jurusan[col_kampus[i]] = data_kampus[0][i]

    result = {}
    result['nilai_user'] = nilai_user
    result['nilai_jurusan'] = nilai_jurusan
    result['rangking_user'] = data_pilihan[0][0]
    result['jumlah_peserta'] = data_kampus[0][13]
    result['kuota_tahun_ini'] = data_kampus[0][14]
    result['peminat_tahun_lalu'] = data_kampus[0][15]
    result['peluang'] = data_pilihan[0][1]
    
    return result

