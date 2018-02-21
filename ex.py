# encoding: utf-8
from flask import Flask, request
import json
#import random
import sys
import pandas as pd
import numpy as np

# set encoding
stdout = sys.stdout
reload(sys)
sys.setdefaultencoding('utf-8')
sys.stdout = stdout



# returns color codes
def get_color(plan, fact):
	if fact > (plan - plan / 10.0):
		return 'G'
	if fact > (plan - plan / 4):
		return 'Y'
	return 'R'

# Flask
app = Flask(__name__)

# load book
xl = pd.ExcelFile("data.xlsx")

# parse shit
df = xl.parse("Лист1")
products = xl.parse("Услуги")
products = products.set_index('DRMS_SERVICE_id')

print 'debug url for region data:'
print 'http://127.0.0.1:5000/biserver/api/v1.0/regions?services[]=1&services[]=2&channels[]=2&channels[]=5&year=2017&months[]=3&months[]=4'
print
print 'debug url for statistic data:'
print 'http://127.0.0.1:5000/biserver/api/v1.0/statistic?year1=2017&year2=2016&months1[]=1&months1[]=2&months2[]=1&months2[]=2&mregion=100&region=54'



@app.route('/biserver/api/v1.0/regions', methods=['GET'])
def get_data():	
	year = 0
	monthes = []
	products = []
	chanels = []
	# parse args
	for a in request.args:
		print "key={}, value={}".format(a, request.args.getlist(a))
		if a == 'months[]':
			for v in request.args.getlist(a):
				monthes.append(int(v))
		elif a == 'services[]':
			for v in request.args.getlist(a):
				products.append(int(v))
		elif a == 'channels[]':
			for v in request.args.getlist(a):
				chanels.append(int(v))
		elif a == 'year':
			year = int(request.args.getlist(a)[0])

	# selection
	q1 = None
	q2 = None
	q3 = None
	q4 = None
	if year > 0:
		q1 = (df.y == year)
		if len(monthes) > 0:
			q2 = (df.m.isin(monthes))
	if len(products) > 0:
		q3 = (df.DRMS_id.isin(products))
	if len(chanels) > 0:
		q4 = (df.ch_id.isin(chanels))
	q = None 
	for qq in [q1, q2, q3, q4]:
	    if not qq is None:
	        if q is None:
	            q = qq
	        else:
	            q = q & qq
	if not q is None:
	    sel = df[q]
	else:
	    sel = df		

	# regions aggregation
	reg = sel.groupby('region_key')
	reg = reg.agg({'plan_val': np.sum, 'fact_val': np.sum}).fillna(0)


	# mrf aggregation
	mrg = sel.groupby('mrf_key')
	mrg = mrg.agg({'plan_val': np.sum, 'fact_val': np.sum}).fillna(0)

	# create return data structure
	regions = []
	for index, row in reg.iterrows():
		regions.append(
			{
			'id':		index,
			'plan':		row['plan_val'],
			'fact':		row['fact_val'],
			'color':	get_color(row['plan_val'], row['fact_val'])
			}
		)
	mregions = []
	for index, row in mrg.iterrows():
		mregions.append(
			{
			'id':		index,
			'plan':		row['plan_val'],
			'fact':		row['fact_val'],
			'color':	get_color(row['plan_val'], row['fact_val'])
			}
		)
	d = {
		'regions':		regions,
		'macroregions':	mregions
		}

	return json.dumps(d)

@app.route('/biserver/api/v1.0/statistic', methods=['GET'])
def stat():
	year1 = 0
	year2 = 0
	monthes1 = []
	monthes2 = []
	mregion = 0
	region = 0
	# parse args
	for a in request.args:
		print "key={}, value={}".format(a, request.args.getlist(a))
		if a == 'months1[]':
			for v in request.args.getlist(a):
				monthes1.append(int(v))
		elif a == 'months2[]':
			for v in request.args.getlist(a):
				monthes2.append(int(v))
		elif a == 'year1':
			year1 = int(request.args.getlist(a)[0])
		elif a == 'year2':
			year2 = int(request.args.getlist(a)[0])
		elif a == 'mregion':
			mregion = int(request.args.getlist(a)[0])
		elif a == 'region':
			region = int(request.args.getlist(a)[0])

	# selection prd1
	q1 = None
	q2 = None
	q3 = None
	q4 = None
	if year1 > 0:
		q1 = (df.y == year1)
		if len(monthes1) > 0:
			q2 = (df.m.isin(monthes1))
	if mregion > 0:
		q3 = (df.mrf_key == mregion)
	if region > 0:
		q4 = (df.region_key == region)
	q = None 
	for qq in [q1, q2, q3, q4]:
	    if not qq is None:
	        if q is None:
	            q = qq
	        else:
	            q = q & qq
	if not q is None:
	    prd1 = df[q]
	else:
	    prd1 = df		
	
	# selection prd2
	q1 = None
	q2 = None
	q3 = None
	q4 = None
	if year2 > 0:
		q1 = (df.y == year2)
		if len(monthes2) > 0:
			q2 = (df.m.isin(monthes2))
	if mregion > 0:
		q3 = (df.mrf_key == mregion)
	if region > 0:
		q4 = (df.region_key == region)
	q = None 
	for qq in [q1, q2, q3, q4]:
	    if not qq is None:
	        if q is None:
	            q = qq
	        else:
	            q = q & qq
	if not q is None:
	    prd2 = df[q]
	else:
	    prd2 = df		

	# agregation
	grouped1 = prd1.groupby('DRMS_id')
	a1 = grouped1.agg({'plan_val': np.sum, 'fact_val': np.sum}).fillna(0)
	grouped2 = prd2.groupby('DRMS_id')
	a2 = grouped2.agg({'plan_val': np.sum, 'fact_val': np.sum}).fillna(0)

	# join periods
	p1p2 = pd.merge(a1, a2, left_index=True, right_index=True, how='outer', suffixes=('_1', '_2'))
	p1p2 = pd.merge(p1p2, products, left_index=True, right_index=True, how='outer')

	# create return data object
	d = []
	for index, row in p1p2.iterrows():
		d.append(
			{
			'product_id'		: index,
			'product_name'		: row['DRMS_SERVICE_NAME'],
			'period1_plan_sum'	: row['plan_val_1'],
			'period2_plan_sum'	: row['plan_val_2'],
			'period1_fact_sum'	: row['fact_val_1'],
			'period2_fact_sum'	: row['fact_val_2']
			}
		)
	h = {'values': d}

	return json.dumps(h)

