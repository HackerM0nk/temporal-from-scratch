export type SpecialistName = 'authorization' | 'injection' | 'secrets';

export interface PullRequestIdentity {
  repository: string;
  pullRequestNumber: number;
}

export interface CommitEvent {
  eventId: string;
  headSha: string;
  baseSha: string;
  receivedAt: string;
}

export interface AgenticReviewInput extends PullRequestIdentity {
  initialCommit: CommitEvent;
  quietPeriodMs?: number;
  maxGenerationsPerRun?: number;
  generation?: number;
  reviewedHeadSha?: string;
  openFindingIds?: string[];
  processedEventIds?: string[];
}

export interface RepositorySnapshot {
  headSha: string;
  mergeBase: string;
  snapshotUri: string;
  contextManifestUri: string;
  changedFiles: string[];
}

export interface ReviewPlan {
  planVersion: string;
  specialists: SpecialistName[];
  maxModelCalls: number;
  maxWallClockSeconds: number;
}

export interface FlueAdmission {
  conversationId: string;
  submissionId: string;
  receipt: string;
  specialist: SpecialistName;
  headSha: string;
}

export interface FindingCandidate {
  specialist: SpecialistName;
  family: string;
  symbol: string;
  location: string;
  rootCause: string;
  evidence: string;
  severity: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL';
}

export interface SpecialistResult {
  admission: FlueAdmission;
  findings: FindingCandidate[];
  promptUri: string;
  transcriptUri: string;
  model: string;
  inputTokens: number;
  outputTokens: number;
}

export interface StableFinding extends FindingCandidate {
  findingId: string;
  evidenceUri: string;
}

export interface ReviewGenerationResult {
  generation: number;
  headSha: string;
  status: 'PUBLISHED' | 'SUPERSEDED' | 'CANCELLED';
  findingIds: string[];
}

export interface AgenticReviewStatus extends PullRequestIdentity {
  lifecycle: 'OPEN' | 'CLOSED';
  phase: 'DEBOUNCING' | 'PREPARING' | 'REVIEWING' | 'JUDGING' | 'PUBLISHING' | 'WAITING' | 'CLOSED';
  latestHeadSha: string;
  reviewedHeadSha?: string;
  generation: number;
  activeSpecialists: SpecialistName[];
  openFindingIds: string[];
}
