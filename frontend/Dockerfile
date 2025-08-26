# syntax=docker/dockerfile:1
FROM node:16 as build
WORKDIR /build
COPY . .

# 1. main 빌드: devDependencies 포함 설치 후 production 빌드
RUN --mount=type=cache,target=/build/main/node_modules \
    cd main && npm install --legacy-peer-deps --include=dev && NODE_ENV=production npm run build

# 2. pc-tool 빌드: devDependencies 포함 설치 후 production 빌드
RUN --mount=type=cache,target=/build/pc-tool/node_modules \
    cd pc-tool && npm install --legacy-peer-deps --include=dev && NODE_ENV=production npm run build

# 3. 필요하면 다른 툴들도 같은 방식으로 주석 해제
# RUN --mount=type=cache,target=/build/text-tool/node_modules \
#     cd text-tool && npm install --legacy-peer-deps --include=dev && NODE_ENV=production npm run build
# RUN --mount=type=cache,target=/build/image-tool/node_modules \
#     cd image-tool && npm install --legacy-peer-deps --include=dev && NODE_ENV=production npm run build

# 4. Nginx로 결과물 복사
FROM nginx:1.22
COPY --from=build /build/dist /usr/share/nginx/html