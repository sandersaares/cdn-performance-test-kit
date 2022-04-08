# Media server supports other paths, as well (just change the "hls" component to publish multiple media streams).
$url = "http://localhost:5005/files/hls/media.m3u8"

# NB! This needs a low latency connection to the media server.
# 1 second segments are the smallest FFmpeg seems to reliably produce for us here. 0.5 fails playback.
$expression = "ffmpeg --% -fflags nobuffer -re -f lavfi -i sine=220:4       -fflags nobuffer -re -f lavfi -i testsrc2=s=1280x720:r=30     -vf `"settb=AVTB,setpts='trunc(PTS/1K)*1K+st(1,trunc(RTCTIME/1K))-1K*trunc(ld(1)/1K)',drawtext=text='%{localtime\:%X}.%{eif\:1M*t-1K*trunc(t*1K)\:d}':x=445:y=10:fontsize=26`"              -map 0 -ac 1 -ar 32000 -c:a aac -b:a 64k       -map 1 -c:v libx264  -pix_fmt yuv420p -profile:v high -preset fast -sc_threshold 0 -x264-params keyint=15:min-keyint=15:rc-lookahead=1:bframes=0:sliced-threads=1 -b:v 1500k -minrate:v 1500k -maxrate:v 1500k -bufsize:v 150k            -f hls -hls_time 1 -hls_list_size 20 -method POST -hls_segment_type fmp4 -http_persistent 1 -ignore_io_errors 1  $url"

Invoke-Expression $expression