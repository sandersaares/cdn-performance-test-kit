$playlist_url = "http://localhost:5005/files/hls/stream_%v/media.m3u8"
$segment_url = "http://localhost:5005/files/hls/stream_%v/segment_%08d.mp4"

# Good reference is https://ottverse.com/hls-packaging-using-ffmpeg-live-vod/

# Requires zero latency localhost connection to origin.
$expression = "ffmpeg --% -fflags nobuffer -re -f lavfi -i sine=220:4       -fflags nobuffer -re -f lavfi -i testsrc2=s=1280x720:r=30     -filter_complex `"settb=AVTB,setpts='trunc(PTS/1K)*1K+st(1,trunc(RTCTIME/1K))-1K*trunc(ld(1)/1K)',drawtext=text='%{localtime\:%X}.%{eif\:1M*t-1K*trunc(t*1K)\:d}':x=445:y=10:fontsize=26,split=4[v0][v1][v2][v3]`"       -map 0 -ac:0 1 -ar:0 32000 -c:a:0 aac -b:a 64k    -map 0 -ac:1 1 -ar:1 32000 -c:a:1 aac -b:a 64k     -map 0 -ac:2 1 -ar:2 32000 -c:a:2 aac -b:a 64k      -map 0 -ac:3 1 -ar:3 32000 -c:a:3 aac -b:a 64k      -map [v0] -c:v:0 libx264  -pix_fmt:v:0 yuv420p -profile:v:0 high -preset:v:0 fast -sc_threshold:v:0 0 -x264-params:v:0 keyint=15:min-keyint=15:rc-lookahead=1:bframes=0:sliced-threads=1 -b:v:0 90k -minrate:v:0 90k -maxrate:v:0 90k -bufsize:v:0 30k -s:v:0 318x318 -r:v:0 7.5 -vsync:v:0 cfr           -map [v1] -c:v:1 libx264  -pix_fmt:v:1 yuv420p -profile:v:1 high -preset:v:1 fast -sc_threshold:v:1 0 -x264-params:v:1 keyint=30:min-keyint=30:rc-lookahead=1:bframes=0:sliced-threads=1 -b:v:1 350k -minrate:v:1 350k -maxrate:v:1 350k -bufsize:v:1 70k -s:v:1 480x480 -r:v:1 15 -vsync:v:1 cfr           -map [v2] -c:v:2 libx264  -pix_fmt:v:2 yuv420p -profile:v:2 high -preset:v:2 fast -sc_threshold:v:2 0 -x264-params:v:2 keyint=60:min-keyint=60:rc-lookahead=1:bframes=0:sliced-threads=1 -b:v:2 850k -minrate:v:2 850k -maxrate:v:2 850k -bufsize:v:2 170k -s:v:2 720x720 -r:v:2 30 -vsync:v:2 cfr          -map [v3] -c:v:3 libx264  -pix_fmt:v:3 yuv420p -profile:v:3 high -preset:v:3 fast -sc_threshold:v:3 0 -x264-params:v:3 keyint=60:min-keyint=60:rc-lookahead=1:bframes=0:sliced-threads=1 -b:v:3 1700k -minrate:v:3 1700k -maxrate:v:3 1700k -bufsize:v:3 340k -s:v:3 960x960 -r:v:3 30 -vsync:v:3 cfr           -f hls -hls_flags independent_segments -hls_time 2 -hls_list_size 10 -method POST -hls_segment_type fmp4 -hls_segment_filename $segment_url -hls_fmp4_init_filename init.mp4  -var_stream_map `"v:0,a:0 v:1,a:1 v:2,a:2 v:3,a:3`" -master_pl_name media.m3u8    -http_persistent 1 -ignore_io_errors 1  $playlist_url"

Invoke-Expression $expression