vertiport specification contains 
1) vertiport id
2) vertiport location in census tract id
3) assumption : {nearest}

Assumption - nearest: means passengers will go to the nearest vertiport and will not consider going to another vertiport for shortest travel time

This will be used to compute spatio-temporal demand data by dividing the Replica trip data into three segments;
1) First mile - trip to origin vertiport in taxi
2) Middle mile - trip from origin to final vertiport in UAM
3) Last mile - trip from final vertiport to final destination in taxi


For the initial network, we assume that the vertiport will be in "Star" network, with one hub and multiple spokes.

