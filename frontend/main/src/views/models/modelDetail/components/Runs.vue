<template>
  <div class="runs">
    <!-- 按钮 -->

    <div class="runs__btn">
      <div class="refresh-btn" @click="handleRefreshTable">
        <Icon icon="charm:refresh" size="18" style="color: #aaa; transform: rotate(-70deg)" />
      </div>
      <!-- <Authority :value="[PermissionCodeEnum.MODEL_RUN]"> -->
      <Button gradient @click="handleOpenRunModel" style="border-radius: 8px" noBorder>
        {{ t('business.models.run.runModel') }}
      </Button>
      <!-- </Authority> -->
    </div>
    <!-- 表格 -->
    <div class="table-wrapper flex-1">
      <BasicTable @register="registerTable" />
    </div>
    <ModelRun
      :classes="props.overviewData?.classes"
      :datasetId="selectId"
      @register="registerRunModel"
      :selectName="selectName"
      :title="title"
      :modelId="props.modelId"
      @run="handleRun"
    >
      <template #select>
        <Select v-model:value="selectId" optionFilterProp="label">
          <Select.Option v-for="item in selectOptions" :key="item.id" @select="handleSelect">
            {{ item.name }}
          </Select.Option>
        </Select>
      </template>
    </ModelRun>
    <RunsDeleteModal @register="registerDeleteModel" :id="deleteId" @delete="reload" />
  </div>
</template>
<script lang="tsx" setup>
  import { h, inject, onBeforeMount, ref, watch } from 'vue';
  import { useI18n } from '/@/hooks/web/useI18n';
  import { RouteEnum, RouteNameEnum } from '/@/enums/routeEnum';
  import { useGo } from '/@/hooks/web/usePage';
  import { useMessage } from '/@/hooks/web/useMessage';
  import { parseParam } from '/@/utils/business/parseParams';
  // 组件
  import { IOverview } from './typing';
  import { Select } from 'ant-design-vue';
  import { Button } from '/@@/Button';
  import { ModelRun } from '/@@/ModelRun';
  import { useModal } from '/@/components/Modal';
  import Icon, { SvgIcon } from '/@/components/Icon/index';
  // import { ApiSelect } from '/@/components/Form/index';
  import { BasicTable, useTable } from '/@/components/Table';
  import RunsDeleteModal from './RunsDeleteModal.vue';
  import { getBasicColumns, getActionColumn } from './tableData';
  // 接口
  import {
    getModelRunApi,
    createModelRunApi,
    rerunModelRunApi,
    getAllDataset,
  } from '/@/api/business/models';
  import {
    ResultsModelParam,
    DataModelParam,
    runModelRunParams,
    ModelRunItem,
  } from '/@/api/business/model/modelsModel';
  import { datasetTypeEnum } from '/@/api/business/model/datasetModel';
  import { useRouter } from 'vue-router';
  import { detailType } from './typing';
  // import { Authority } from '/@/components/Authority';
  // import { PermissionCodeEnum } from '/@/enums/permissionCodeEnum';
  import { onBeforeUnmount } from 'vue';
  // polling 관리용
  let pollTimer: any = null;
  function startPolling() {
    stopPolling(); // 기존 polling 초기화
    pollTimer = setInterval(async () => {
      try {
        const res = await reload(); // reload 호출
        // console.log(" reload 결과:", res);

        const allDone = res.every((r) => r.completionRate === 1 && r.status === "SUCCESS");
        // const hasError = res.some(
        //   (r) => r.status === "SUCCESS_WITH_ERROR" || r.status === "FAILURE"
        // );

        // if (allDone || hasError) {
        if (allDone){
          console.log("모든 run 완료 또는 오류 발생 → polling 중지");
          stopPolling();
        }
      } catch (e) {
        console.error("Polling 중 reload 에러 발생:", e);
      }
    }, 5000);
  }
  function stopPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  // 컴포넌트 언마운트 시 polling 정리, ex) 다른 메뉴를 클릭 시, status 체크 중지
  onBeforeUnmount(() => {
    // console.log("onBeforeUnmount에 의한 polling 초기화");
    // stopPolling();
  });

  const { t } = useI18n();
  const go = useGo();
  const { createMessage, createConfirm } = useMessage();

  const props = defineProps<{
    modelId: string;
    datasetType: datasetTypeEnum;
    isLimit: boolean;
    overviewData: IOverview;
  }>();
  const emits = defineEmits(['reload', 'setActiveKey']);

  // Table ==>
  const [registerTable, { reload }] = useTable({
    afterFetch: (res) => {
      res.forEach((r, i) => {
        console.log(`🔎 row[${i}] completionRate:`, r.completionRate, "status:", r.status);
      });
      const allDone = res.every(
        (r) => r.completionRate === 1 && r.status === "SUCCESS"
      );
      // const hasError = res.some(
      //   (r) => r.status === "SUCCESS_WITH_ERROR" || r.status === "FAILURE"
      // );

      if (allDone){ //|| hasError) {
        console.log("모든 run이 완료되었거나 오류 발생 → polling 중지");
        stopPolling();
      }

      return res;
    },
    beforeFetch: (res) => {
      res?.status && (res.status = res.status?.toString());
      res?.runRecordType && (res.runRecordType = res.runRecordType?.toString());
      for (const key in res) {
        if (Object.prototype.hasOwnProperty.call(res, key)) {
          const element = res[key];
          if (!element) {
            delete res[key];
          }
        }
      }
      if (res?.datasetName) {
        let pa = res?.datasetName.map((i) => i.value);
        delete res.datasetName;
        res.datasetIds = pa.toString();
      }
      return res;
    },
    // defSort: {
    //   field: 'ascOrDesc',
    //   order: 'ascend',
    // },
    sortFn: (sortInfo) => {
      if (!sortInfo?.order) {
        return;
      }
      let trans = sortInfo.order.split('end')[0].toUpperCase();
      return { ascOrDesc: trans };
    },
    bordered: true,
    api: getModelRunApi,
    columns: getBasicColumns(),
    searchInfo: { modelId: Number(props.modelId) },
    showIndexColumn: false,
    pagination: true,
    actionColumn: getActionColumn({
      view: handleView,
      delete: handleDelete,
      rerun: handleRerun,
    }),
  });

  // 刷新表格
  const handleRefreshTable = () => {
    reload();
    emits('reload');
  };

  // ModelRun
  const selectName = t('business.models.runModel.dataset');
  const title = t('business.models.run.runModel');
  const [
    registerRunModel,
    { openModal: openRunModal, closeModal: closeRunModal, setModalProps: setRunModalProps },
  ] = useModal();
  // 打开 ModelRun 弹窗

  let overviewData: any = inject('overviewData');

  let IconStatus = ({ success }) => {
    if (!!success) {
      return <SvgIcon name="sucess"></SvgIcon>;
    } else {
      return <SvgIcon name="error"></SvgIcon>;
    }
  };
  let warningContent = ({ hasClass, hasUrl }) => (
    <div>
      {' '}
      {t('business.models.run.runModelWarning')}
      <div>
        <IconStatus style="display: inline;" success={hasClass} />{' '}
        {t('business.models.run.runModelWarningClass')}{' '}
      </div>
      <div>
        <IconStatus style="display: inline;" success={hasUrl} />{' '}
        {t('business.models.run.runModelWarningConfig')}{' '}
      </div>
    </div>
  );
  const router = useRouter();
  const handleOpenRunModel = () => {
    if (overviewData.classes.length < 1 || !overviewData.url) {
      let warningConfig = {
        hasClass: overviewData.classes.length >= 1,
        hasUrl: !!overviewData.url,
      };
      createConfirm({
        iconType: 'warning',
        title: () => h('span', t('business.models.deleteModel.title')),
        content: () => <warningContent {...warningConfig}></warningContent>,
        okText: 'Set up',
        onOk: async () => {
          let tabId = detailType.overview;
          if (warningConfig.hasClass && !warningConfig.hasUrl) {
            tabId = detailType.settings;
          }
          // 跳转后 切换tab window.history.pushState 路由不更新 导致仍然认为在当前页，无法切回来
          // let query = { id: router.currentRoute.value.query.id, tabId };
          // const params = { tabId: 'RUNS' };
          // router.push({
          //   name: RouteNameEnum.MODEL_DETAIL,
          //   query,
          //   // params,
          // });

          emits('setActiveKey', tabId);
        },
        okButtonProps: {
          style: { background: '#60a9fe', 'border-radius': '6px', padding: '10px 16px' },
        } as any,
      });
    } else {
      openRunModal(true, {});
    }

    // openRunModal(true, {});
  };
  // 执行 RunModel
  const handleRun = async (result: Nullable<ResultsModelParam>, data: Nullable<DataModelParam>) => {
    console.log("handleRun 실행 (Runs.vue)");
    if (props.isLimit) {
      createMessage.error(
        'model runs has reached maximum limit, please contact us for more model usage',
      );
      setRunModalProps({ confirmLoading: false });
      return;
    }

    try {
      const modelId = Number(props.modelId);
      const datasetId = Number(selectId.value);

      const runParams: runModelRunParams = {
        modelId: modelId,
        datasetId: datasetId,
        resultFilterParam: result,
        dataFilterParam: data,
      };

      // return;
      if (!datasetId) {
        createMessage.error(t('business.models.runModel.selectDataset'));
        return;
      }
      await createModelRunApi(runParams);

      setTimeout(() => {
        closeRunModal();
        setRunModalProps({ confirmLoading: false });
        reload();
        // 이후 RUNNING 있으면 polling 시작
        startPolling();
      }, 800);
    } catch (error: unknown) {
      createMessage.error(String(error));
      setTimeout(() => {
        setRunModalProps({ confirmLoading: false });
      }, 800);
    }
  };

  // 下拉框 ===>
  onBeforeMount(() => {
    getSelectOptions();
  });
  // 调整为监听到 datasetType 变化
  watch(
    () => props.datasetType,
    () => {
      getSelectOptions();
    },
  );
  const selectId = ref<string | number>('');
  const selectOptions = ref();
  // 获取下拉框数据
  const getSelectOptions = async () => {
    let datasetType = [datasetTypeEnum.IMAGE];
    if (props.datasetType != datasetTypeEnum.IMAGE) {
      datasetType = [datasetTypeEnum.LIDAR_BASIC, datasetTypeEnum.LIDAR_FUSION];
    }
    const res = await getAllDataset({ datasetTypes: datasetType.toString() });
    selectOptions.value = res;
    selectId.value = selectOptions.value?.[0]?.id;
  };
  // 下拉框选择事件
  const handleSelect = (e) => {
    selectId.value = e;
  };

  // rerun 事件
  async function handleRerun(record: ModelRunItem) {
    if (props.isLimit) {
      createMessage.error(
        'model runs has reached maximum limit, please contact us for more model usage',
      );
      return;
    }
    await rerunModelRunApi({ id: record.id });
    setTimeout(() => {
      reload();
      startPolling();
    }, 800);
  }

  // view 事件
  function handleView(record: ModelRunItem) {
    const url = `${RouteEnum.DATASETS}/data`;
    const params = { id: record.datasetId };
    go(parseParam(url, params));
  }

  // delete 事件
  const [registerDeleteModel, { openModal: openRunsDeleteModal }] = useModal();
  const deleteId = ref('');
  function handleDelete(record: ModelRunItem) {
    deleteId.value = String(record.id);
    openRunsDeleteModal();
  }
</script>
<style lang="less" scoped>
  .runs {
    position: relative;
    display: flex;
    flex-direction: column;
    // height: 100%;

    &__btn {
      position: absolute;
      top: -65px;
      right: 0px;
      display: flex;
      gap: 10px;
      height: 36px;
      margin-bottom: 20px;
      flex-direction: row-reverse;

      button {
        height: 36px;
      }

      .refresh-btn {
        width: 44px;
        height: 36px;
        display: flex;
        justify-content: center;
        align-items: center;
        border-radius: 8px;
        background-color: #fff;
        border: 4px solid #f3f3f3;
        cursor: pointer;
      }
    }
  }

  .table__status {
    display: flex;
    justify-content: center;
    align-items: center;
  }

  :deep(.ant-progress) {
    .ant-progress-text {
      color: #333;
    }
  }

  :deep(.ant-table) {
    .ant-table-column-title {
      span {
        font-weight: 700 !important;
        color: #666666;
      }
    }
  }
</style>
