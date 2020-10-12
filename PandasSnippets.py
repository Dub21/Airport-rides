def calculate_distance(df):
    """
    from latitude and longitude, calculates Euclidean
    distance between pickup and dropoff
    """
    earth_radius = 6373.0  # Earth radius (Km)

    dlon = np.radians((df[6]).values - (df[3]).values)
    dlat = np.radians((df[5]).values - (df[2]).values)

    a = np.power(np.sin(dlat / 2), 2) + np.cos(np.radians((df[5]).values
        )) * np.cos(np.radians(
            (df[2]).values)) * np.power(np.sin(dlon / 2), 2)
    
    return earth_radius * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

if __name__ == '__main__':
    df = pd.read_csv("/Users/dub/Downloads/2010_03.trips.csv", sep=" ",header=None )
    
    df['distance'] = calculate_distance(df)

    df.to_csv("/Users/dub/Downloads/2010_03.trips.csv", index=False)
    
    print("Median:",df.distance.median())
    print("Mean:",df.distance.mean())
    print("Max Trip Distance:",df.distance.max())
    print("Min Trip Distance:",df.distance.min())
    print("Standard Deviation:",df.distance.std())
    
    fig,ax = plot.subplots(1,3,figsize = (15,3)) 
    
    df.distance.hist(bins=144,ax=ax[0])
    ax[0].set_xlabel('Trip Distance (miles)')
    ax[0].set_ylabel('Frequency')
    ax[0].set_yscale('symlog')
    ax[0].set_title('Original Trip Distance')
    
    trip_Distance_ = df.distance 
    median_Trip_Distance_=trip_Distance_.median()
    std_Trip_Distance_=trip_Distance_.std()
    
    trip_Distance_= df.distance[df.distance>0]
    trip_Distance_ = trip_Distance_[(trip_Distance_-median_Trip_Distance_).abs()<3*std_Trip_Distance_]
    
    trip_Distance_.hist(bins=144,ax=ax[1])
    ax[1].set_xlabel('Trip Distance (km)')
    ax[1].set_ylabel('Frequency')
    ax[1].set_title('Trip length distribution without outliers')
    
    trip_Distance_.median()