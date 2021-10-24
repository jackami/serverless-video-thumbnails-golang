clean:
	rm -rf build
	rm -rf bin

build: clean
	mkdir bin
	mkdir build
	cd build && curl https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz | tar xJ
	cd ..
	mv build/ffmpeg*/ffmpeg build/ffmpeg*/ffprobe ./bin/
	zip -r ffmpeg.zip bin/
