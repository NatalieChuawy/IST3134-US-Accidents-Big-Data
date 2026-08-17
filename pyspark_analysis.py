import argparse
import time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, DoubleType

SELECTED_COLUMNS = [
    'ID','Severity','Start_Time','End_Time','City','State','Temperature(F)',
    'Humidity(%)','Visibility(mi)','Wind_Speed(mph)','Weather_Condition',
    'Sunrise_Sunset','Traffic_Signal','Junction'
]

def args():
    p=argparse.ArgumentParser()
    p.add_argument('--input',required=True)
    p.add_argument('--output',required=True)
    return p.parse_args()

def ts(c):
    return F.coalesce(
        F.to_timestamp(c,'yyyy-MM-dd HH:mm:ss.SSSSSS'),
        F.to_timestamp(c,'yyyy-MM-dd HH:mm:ss.SSS'),
        F.to_timestamp(c,'yyyy-MM-dd HH:mm:ss')
    )

def save(df,path):
    df.coalesce(1).write.mode('overwrite').option('header',True).csv(path)

def main():
    a=args()
    spark=SparkSession.builder.appName('IST3134-US-Accidents').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    print('Spark version:',spark.version)
    print('Spark master:',spark.sparkContext.master)
    print('Default parallelism:',spark.sparkContext.defaultParallelism)

    t=time.perf_counter()
    raw=(spark.read.option('header',True).option('inferSchema',True)
         .option('mode','PERMISSIVE').csv(a.input))
    raw_n=raw.count()
    load_time=time.perf_counter()-t
    print('Raw rows:',raw_n,'Raw columns:',len(raw.columns),'Load+count seconds:',round(load_time,3))
    raw.printSchema()

    missing=[c for c in SELECTED_COLUMNS if c not in raw.columns]
    if missing:
        raise ValueError('Missing selected columns: '+', '.join(missing))

    df=raw.select(*SELECTED_COLUMNS)

    dup=(df.groupBy('ID').count().filter('count > 1')
         .agg(F.sum(F.col('count')-1).alias('n')).first()['n']) or 0
    print('Duplicate rows based on ID:',int(dup))

    nulls=df.agg(*[
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast('string'))==''),1).otherwise(0)).alias(c)
        for c in SELECTED_COLUMNS
    ])
    nulls.show(truncate=False)
    save(nulls,f'{a.output}/missing_values')

    d=(df.dropDuplicates(['ID'])
       .withColumn('Severity',F.col('Severity').cast(IntegerType()))
       .withColumn('Temperature_F',F.col('Temperature(F)').cast(DoubleType()))
       .withColumn('Humidity_Pct',F.col('Humidity(%)').cast(DoubleType()))
       .withColumn('Visibility_Mi',F.col('Visibility(mi)').cast(DoubleType()))
       .withColumn('Wind_Speed_Mph',F.col('Wind_Speed(mph)').cast(DoubleType()))
       .withColumn('Start_TS',ts('Start_Time'))
       .withColumn('End_TS',ts('End_Time'))
       .withColumn('Year',F.year('Start_TS'))
       .withColumn('Month',F.month('Start_TS'))
       .withColumn('Hour',F.hour('Start_TS'))
       .withColumn('DayOfWeek',F.date_format('Start_TS','EEEE'))
       .withColumn('Duration_Minutes',(F.col('End_TS').cast('long')-F.col('Start_TS').cast('long'))/60.0)
       .filter(F.col('Severity').between(1,4)))
    clean_n=d.count()
    print('Cleaned rows:',clean_n)

    # Spatial
    states=(d.filter(F.col('State').isNotNull()).groupBy('State').count()
            .withColumnRenamed('count','Accident_Count').orderBy(F.desc('Accident_Count')))
    cities=(d.filter(F.col('City').isNotNull() & F.col('State').isNotNull())
            .groupBy('City','State').count().withColumnRenamed('count','Accident_Count')
            .orderBy(F.desc('Accident_Count')))
    save(states.limit(10),f'{a.output}/top_10_states')
    save(cities.limit(10),f'{a.output}/top_10_cities')
    states.show(10,False); cities.show(10,False)

    # Temporal
    hourly=d.filter(F.col('Hour').isNotNull()).groupBy('Hour').count().withColumnRenamed('count','Accident_Count').orderBy('Hour')
    monthly=d.filter(F.col('Month').isNotNull()).groupBy('Month').count().withColumnRenamed('count','Accident_Count').orderBy('Month')
    yearly=d.filter(F.col('Year').isNotNull()).groupBy('Year').count().withColumnRenamed('count','Accident_Count').orderBy('Year')
    weekday=d.filter(F.col('DayOfWeek').isNotNull()).groupBy('DayOfWeek').count().withColumnRenamed('count','Accident_Count').orderBy(F.desc('Accident_Count'))
    save(hourly,f'{a.output}/accidents_by_hour'); save(weekday,f'{a.output}/accidents_by_weekday')
    save(monthly,f'{a.output}/accidents_by_month'); save(yearly,f'{a.output}/accidents_by_year')

    # Weather/environment
    weather=(d.filter(F.col('Weather_Condition').isNotNull()).groupBy('Weather_Condition').count()
             .withColumnRenamed('count','Accident_Count').orderBy(F.desc('Accident_Count')))
    env=(d.groupBy('Severity').agg(
        F.count('*').alias('Records'),
        F.round(F.avg('Temperature_F'),2).alias('Avg_Temperature_F'),
        F.round(F.avg('Humidity_Pct'),2).alias('Avg_Humidity_Pct'),
        F.round(F.avg('Visibility_Mi'),2).alias('Avg_Visibility_Mi'),
        F.round(F.avg('Wind_Speed_Mph'),2).alias('Avg_Wind_Speed_Mph')).orderBy('Severity'))
    save(weather.limit(10),f'{a.output}/top_10_weather_conditions')
    save(env,f'{a.output}/environmental_by_severity')

    # Severity and day/night
    sev=(d.groupBy('Severity').count().withColumnRenamed('count','Accident_Count')
         .withColumn('Percentage',F.round(F.col('Accident_Count')/F.lit(clean_n)*100,2)).orderBy('Severity'))
    daynight=(d.filter(F.col('Sunrise_Sunset').isin('Day','Night')).groupBy('Sunrise_Sunset').count()
              .withColumnRenamed('count','Accident_Count').orderBy(F.desc('Accident_Count')))
    save(sev,f'{a.output}/severity_distribution'); save(daynight,f'{a.output}/day_night_distribution')

    # Road features
    junction=(d.filter(F.col('Junction').isNotNull()).groupBy('Junction').count().withColumnRenamed('count','Accident_Count'))
    signal=(d.filter(F.col('Traffic_Signal').isNotNull()).groupBy('Traffic_Signal').count().withColumnRenamed('count','Accident_Count'))
    save(junction,f'{a.output}/junction_distribution'); save(signal,f'{a.output}/traffic_signal_distribution')

    # Duration uses End_Time
    duration=(d.filter(F.col('Duration_Minutes').between(0,1440)).agg(
        F.count('*').alias('Valid_Duration_Records'),
        F.round(F.avg('Duration_Minutes'),2).alias('Avg_Duration_Minutes'),
        F.round(F.expr('percentile_approx(Duration_Minutes,0.5)'),2).alias('Median_Duration_Minutes')))
    save(duration,f'{a.output}/duration_summary')

    # Spark SQL validation
    d.createOrReplaceTempView('accidents')
    sql_states=spark.sql('''
      SELECT State, COUNT(*) AS Accident_Count
      FROM accidents
      WHERE State IS NOT NULL
      GROUP BY State
      ORDER BY Accident_Count DESC
      LIMIT 10
    ''')
    save(sql_states,f'{a.output}/spark_sql_top_10_states')
    print('SQL matches DataFrame:',[(r.State,r.Accident_Count) for r in sql_states.collect()]==[(r.State,r.Accident_Count) for r in states.limit(10).collect()])

    print('\nFORMATTED PHYSICAL PLAN')
    states.explain(mode='formatted')
    spark.stop()

if __name__=='__main__':
    main()
